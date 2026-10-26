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
from pipeline.guarded_normalization import MODE as GUARDED_MODE, MODES as GUARDED_MODES, PREFIX_MODES, prepare_sql as prepare_guarded, fast_statement as guarded_statement, validation_destinations
from pipeline.normalization import MODES as NORMALIZATION_MODES, prepare_sql as normalize_sql, rewrite as normalized_statement
from pipeline.recipes import MODES, ORDER, BULK_MODES, bulk_unique_tables,CTAS_MODES
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
    yield {"name":"single","edits":[("numeric_column_13",[])],"remove":"text_column_1"}
    yield {"name":"multi","edits":[("numeric_column_5","x"),("text_column_2","-32769"),("numeric_column_9","1e400"),("numeric_column_23",2),("numeric_column_22","1.2")],"settings":2}
    yield {"name":"multi","edits":[("numeric_column_5",{"a":1}),("id_column_1",{}),("numeric_column_13",None),("text_column_2","?"),("integer_column_13","Infinity")]}
    yield {"name":"sparse-hole","edits":[('integer_column_15','14'),('text_column_17','Z'),('numeric_column_29','1'),('numeric_column_30','0')]}
    for declared in (None,'-1','9999'):
        yield {"name":"declared-bound","edits":[(f'integer_column_{n}',declared) for n in range(2,16)]}
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
        guarded_safe=None
        if mode in GUARDED_MODES:
            conn.execute(prepare_guarded(scopes,mode))
            guarded_safe=conn.execute("SELECT safe FROM pg_temp.pipeline_normalization_domain").fetchone()[0]
        if mode in BULK_MODES and len(statements)>1 and (mode not in GUARDED_MODES or guarded_safe):
            from pipeline.bulk_fks import prepare_sql,restore_sql
            conn.execute("LOCK TABLE public.raw_data IN SHARE MODE")
            conn.execute(prepare_sql())
        if mode in ("hash_array64","hash_array64_nomemo","bulk_full64","bulk_normalize64"):
            conn.execute("SET LOCAL work_mem='64MB'")
        if mode=="analyze_base" and len(statements)==1:
            conn.execute("ANALYZE public.table1")
        if mode in NORMALIZATION_MODES and len(statements)==1:conn.execute(normalize_sql(scopes))
        for statement in statements:
            if mode in GUARDED_MODES and guarded_safe:statement=guarded_statement(statement,scopes,mode)
            if mode in NORMALIZATION_MODES:
                if statement.startswith("INSERT INTO public.table1 ("):conn.execute(normalize_sql(scopes))
                statement=normalized_statement(statement,scopes)
            unique_table=next((t for t in bulk_unique_tables(mode) if statement.startswith(f"INSERT INTO public.{t} ")),None) if len(statements)>1 and (mode not in GUARDED_MODES or guarded_safe) else None
            if unique_table:
                from pipeline.bulk_indexes import prepare_mode_sql as prepare_index, restore_sql as restore_index
                conn.execute(prepare_index(mode,unique_table))
            ctas_cleanup=None
            if mode in CTAS_MODES and guarded_safe:
                from pipeline.parallel_normalization import materialize_statement
                prep,statement,ctas_cleanup=materialize_statement(statement,mode=mode)
                conn.execute(prep)
            original_mergejoin=None
            if mode in ("hash_table1","qf_hash1","hash_array","hash_array64","hash_array64_nomemo") and statement.startswith("INSERT INTO public.table1 "):
                original_mergejoin=conn.execute("SHOW enable_mergejoin").fetchone()[0]
                conn.execute("SET LOCAL enable_mergejoin=off")
            original_memoize=None
            if mode=="hash_array64_nomemo" and statement.startswith("INSERT INTO public.table2 "):
                original_memoize=conn.execute("SHOW enable_memoize").fetchone()[0]
                conn.execute("SET LOCAL enable_memoize=off")
            if mode=="guarded_qf" and statement.startswith("INSERT INTO public.table2"):
                from pipeline.guards import choose
                statement,_=choose(conn,statement,scopes)
            conn.execute(statement)
            if ctas_cleanup:conn.execute(ctas_cleanup)
            if unique_table:conn.execute(restore_index(unique_table,mode=mode))
            if original_mergejoin is not None:
                conn.execute("SELECT set_config('enable_mergejoin',%s,true)",(original_mergejoin,))
            if original_memoize is not None:
                conn.execute("SELECT set_config('enable_memoize',%s,true)",(original_memoize,))
            maintained=("table3","table4","table1") if mode=="analyze_base" else ("table3","table4") if mode=="analyze_dims" else ()
            if mode in PREFIX_MODES and guarded_safe:maintained=('table3','table4')
            for table in maintained:
                if statement.startswith(f"INSERT INTO public.{table} "):
                    conn.execute(f"ANALYZE public.{table}")
        if mode in BULK_MODES and len(statements)>1 and (mode not in GUARDED_MODES or guarded_safe):
            conn.execute(restore_sql())
        if mode in CTAS_MODES:conn.execute('DROP TABLE challenge_lab.pipeline_normalized_input')
        outputs = {table: [r[0] for r in conn.execute(f"SELECT value::text FROM ({canonical(table)}) c ORDER BY value::text")] for table in tables}
        return {"sqlstate": "00000", "outputs": outputs, "transaction_state": conn.info.transaction_status.name,"safe_domain":guarded_safe}
    except psycopg.Error as error:
        return {"sqlstate": error.sqlstate, "error_class": type(error).__name__,
                "message": error.diag.message_primary, "transaction_state": conn.info.transaction_status.name,"safe_domain":guarded_safe}
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
    parser.add_argument("--allow-mismatches",action="store_true",help="Explicit negative-experiment mode; default exits nonzero on semantic differences")
    args = parser.parse_args()
    args.modes = list(dict.fromkeys(m for m in args.modes if m!="baseline"))
    db = args.database or "lab_semantics_" + uuid.uuid4().hex[:8]
    if not args.database:
        create_fixture(db)
    report = {"kind": "mutated_functional_tests", "seed": args.seed, "database": db,
              "strict_policy": "same acceptance, exact outputs and SQLSTATE; exact primary message for single-cell cases; transaction state equal",
              "runs": [], "mismatches": [],"skipped_scopes":[]}
    with connect(db, autocommit=True) as conn:
        require_lab(conn)
        report["environment"] = environment(conn)
        scopes = [r[0] for r in conn.execute("SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1")]
        sqls = {mode: prepared(mode, scopes) for mode in ["baseline"] + args.modes}
        for mode,recipes in sqls.items():
            validate_functions(conn, validation_destinations(recipes) if mode in GUARDED_MODES else recipes)
        conn.execute("TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY")
        for d in sqls["baseline"][:-1]:
            for s in d["statements"]:
                conn.execute(s)
        raw_id, original = conn.execute("SELECT raw_id,raw FROM public.raw_data ORDER BY raw_id LIMIT 1").fetchone()
        for scope in ("table2_only", "full_pipeline"):
            active_sqls=sqls
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
                               for mode, recipes in active_sqls.items()}
                    base = outputs.pop("baseline")
                    for mode, result in outputs.items():
                        equal = base["sqlstate"] == result["sqlstate"] and base.get("outputs") == result.get("outputs") and base["transaction_state"] == result["transaction_state"]
                        if case["name"] == "single":
                            equal = equal and base.get("message") == result.get("message")
                        record = {"scope": scope,"case":index,"mode":mode,"name":case["name"],
                                  "baseline_sqlstate":base["sqlstate"],"candidate_sqlstate":result["sqlstate"],"equal":equal,
                                  "baseline_transaction_state":base["transaction_state"],"candidate_transaction_state":result["transaction_state"]}
                        record['candidate_safe_domain']=result.get('safe_domain')
                        report["runs"].append(record)
                        if not equal:
                            report["mismatches"].append({**record,"mutation":case,"baseline":base,"candidate":result})
                if index % 100 == 0:
                    print(f"{scope}: {index}; mismatches={len(report['mismatches'])}",flush=True)
                    write_report(args.output,report)
        report["summary"] = {mode: {"cases":sum(r["mode"]==mode for r in report["runs"]),
                                     "mismatches":sum(r["mode"]==mode for r in report["mismatches"]),
                                     "fast_domain_cases":sum(r['mode']==mode and r.get('candidate_safe_domain') is True for r in report['runs']),
                                     "fallback_cases":sum(r['mode']==mode and r.get('candidate_safe_domain') is False for r in report['runs'])} for mode in args.modes}
        write_report(args.output,report)
        print(json.dumps(report["summary"],indent=2),flush=True)
        if report['mismatches'] and not args.allow_mismatches:raise SystemExit(1)


if __name__ == "__main__":
    main()
