"""SQLSTATE-strict differential fuzzing on isolated, deliberately mutated inputs."""
import argparse
import copy
import json
import random
import uuid
import psycopg
from psycopg.types.json import Jsonb

from bench.common import connect, require_lab, write_report, environment
from bench.fixture import create_fixture
from bench.run import prepared, validate_functions
from pipeline.recipes import MODES, ORDER
from tests.equivalence.oracle import canonical

VALUES = [None, "", " ", " 2 ", "nope", "32768", "-32769", "-1", "-0.1", "1.2", "NaN",
          "Infinity", "1e400", "9"*45, 2, 1.5, True, [], {}, ["X"], {"a": 1}, "x", "?", "Z"]
SETTINGS = [{}, {"enable_nestloop": "off"}, {"enable_hashjoin": "off", "enable_mergejoin": "off"},
            {"work_mem": "64kB", "enable_material": "off"}]


def cases(seed, count):
    yield {"name": "unchanged", "edits": []}
    for shape in [None, [], "text", 42, True, {}, [1,2]]:
        yield {"name": "values-shape", "shape": shape}
    yield {"name": "missing-values", "missing_values": True}
    # Known semantic traps: apparently unused axes and competing cast errors.
    for edits in [
        [("integer_column_15", ""), ("numeric_column_29", "bad"), ("text_column_17", "X")],
        [("integer_column_15", "bad"), ("numeric_column_29", "-1"), ("text_column_17", "?")],
        [("integer_column_2", "40000"), ("numeric_column_3", "bad"), ("text_column_4", "X")],
        [("integer_column_2", "1"), ("numeric_column_3", "1e400"), ("numeric_column_4", "bad")],
        [("text_column_2", ["X"]), ("numeric_column_7", []), ("integer_column_1", True), ("numeric_column_19", None)],
    ]:
        yield {"name": "multi-error-trap", "edits": edits}
    keys = [f"integer_column_{i}" for i in range(1,16)] + [f"text_column_{i}" for i in range(1,18)] + [f"numeric_column_{i}" for i in range(1,31)] + ["id_column_1", "datetime_column_1"]
    rng = random.Random(seed)
    for n in range(count):
        size = 1 if n % 3 else rng.randint(2, 5)
        yield {"name": "single" if size == 1 else "multi", "edits": [(rng.choice(keys), rng.choice(VALUES)) for _ in range(size)],
               "remove": rng.choice(keys) if n % 7 == 0 else None, "settings": n % len(SETTINGS)}


def outcome(conn, statements, tables,mode=None,scopes=None):
    conn.execute("SAVEPOINT trial")
    try:
        for statement in statements:
            if mode=="guarded_qf" and statement.startswith("INSERT INTO public.table2"):
                from pipeline.guards import choose
                statement,_=choose(conn,statement,scopes)
            conn.execute(statement)
        outputs = {table: [r[0] for r in conn.execute(f"SELECT value::text FROM ({canonical(table)}) c ORDER BY value::text")] for table in tables}
        return {"sqlstate": "00000", "outputs": outputs, "transaction_state": conn.info.transaction_status.name}
    except psycopg.Error as error:
        return {"sqlstate": error.sqlstate, "error_class": type(error).__name__,
                "message": error.diag.message_primary, "transaction_state": conn.info.transaction_status.name}
    finally:
        conn.execute("ROLLBACK TO SAVEPOINT trial")
        conn.execute("RELEASE SAVEPOINT trial")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database")
    parser.add_argument("--count", type=int, default=400)
    parser.add_argument("--seed", type=int, default=718)
    parser.add_argument("--modes", nargs="+", choices=MODES, default=["payload_once", "lazy_keys", "record_once", "path_once"])
    parser.add_argument("--output", default="reports/semantics.json")
    args = parser.parse_args()
    args.modes = list(dict.fromkeys(m for m in args.modes if m!="baseline"))
    db = args.database or "lab_semantics_" + uuid.uuid4().hex[:8]
    if not args.database:
        create_fixture(db)
    report = {"kind": "mutated_functional_tests", "seed": args.seed, "database": db,
              "strict_policy": "same acceptance, exact outputs and SQLSTATE; exact primary message for single-cell cases; transaction state equal",
              "runs": [], "mismatches": []}
    with connect(db, autocommit=True) as conn:
        require_lab(conn)
        report["environment"] = environment(conn)
        scopes = [r[0] for r in conn.execute("SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1")]
        sqls = {mode: prepared(mode, scopes) for mode in ["baseline"] + args.modes}
        for recipes in sqls.values():
            validate_functions(conn, recipes)
        conn.execute("TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY")
        for d in sqls["baseline"][:-1]:
            for s in d["statements"]:
                conn.execute(s)
        raw_id, original = conn.execute("SELECT raw_id,raw FROM public.raw_data ORDER BY raw_id LIMIT 1").fetchone()
        for scope in ("table2_only", "full_pipeline"):
            for index, case in enumerate(cases(args.seed, args.count)):
                with conn.transaction(force_rollback=True):
                    for key, value in SETTINGS[case.get("settings",0)].items():
                        conn.execute("SELECT set_config(%s,%s,true)", (key,value))
                    raw = copy.deepcopy(original)
                    if "shape" in case:
                        raw["values"] = case["shape"]
                    elif case.get("missing_values"):
                        raw.pop("values",None)
                    else:
                        for key, value in case.get("edits",[]):
                            raw["values"][key] = value
                        raw["values"].pop(case.get("remove"),None)
                    conn.execute("UPDATE public.raw_data SET raw=%s WHERE raw_id=%s", (Jsonb(raw),raw_id))
                    if scope == "full_pipeline":
                        conn.execute("TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY")
                    start = -1 if scope == "table2_only" else 0
                    tables = ("table2",) if start == -1 else ORDER
                    outputs = {mode: outcome(conn,[s for d in recipes[start:] for s in d["statements"]],tables,mode,scopes)
                               for mode, recipes in sqls.items()}
                    base = outputs.pop("baseline")
                    for mode, result in outputs.items():
                        equal = base["sqlstate"] == result["sqlstate"] and base.get("outputs") == result.get("outputs") and base["transaction_state"] == result["transaction_state"]
                        if case["name"] == "single":
                            equal = equal and base.get("message") == result.get("message")
                        record = {"scope": scope,"case":index,"mode":mode,"name":case["name"],
                                  "baseline_sqlstate":base["sqlstate"],"candidate_sqlstate":result["sqlstate"],"equal":equal,
                                  "baseline_transaction_state":base["transaction_state"],"candidate_transaction_state":result["transaction_state"]}
                        report["runs"].append(record)
                        if not equal:
                            report["mismatches"].append({**record,"mutation":case,"baseline":base,"candidate":result})
                if index % 100 == 0:
                    print(f"{scope}: {index}; mismatches={len(report['mismatches'])}",flush=True)
                    write_report(args.output,report)
        report["summary"] = {mode: {"cases":sum(r["mode"]==mode for r in report["runs"]),
                                     "mismatches":sum(r["mode"]==mode for r in report["mismatches"])} for mode in args.modes}
        write_report(args.output,report)
        print(json.dumps(report["summary"],indent=2),flush=True)


if __name__ == "__main__":
    main()
