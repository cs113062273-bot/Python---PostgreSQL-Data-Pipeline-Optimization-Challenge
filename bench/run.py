"""Alternating SQL diagnostics with server timing and six-table equivalence.

NOT MEASURED PROCESSING TOTAL: excludes full GUI, preflight, counters, versions,
dumps and SSH. Exact comparison/reference creation are outside timed stages.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import statistics
import time
import threading
import psutil

from bench.common import ROOT, connect, environment, require_lab, write_report
from pipeline.recipes import MODES, ORDER, destinations
from pipeline.upstream import PostgresAdminService
from tests.equivalence.oracle import (assert_exact, assert_relationships, fingerprints,
                                      schema_signature, snapshot)


def prepared(mode, scopes):
    return PostgresAdminService._prepare_cross_table_expansion("public.raw_data", scopes, destinations(mode))["destinations"]


def validate_functions(conn, recipes):
    service = PostgresAdminService()
    service.sql_username = "diagnostic"
    def transport(command, **kwargs):
        import shlex
        argv = shlex.split(command)
        rows = conn.execute(argv[argv.index("-c")+1]).fetchall()
        return "\n".join(str(row[0]) for row in rows)
    service.run_remote_command = transport
    # The organizer rejects stable functions, including jsonb_to_record (its
    # volatility covers arbitrary target input functions). Keep that rejection
    # intact in the application. Only in this disposable laboratory, authorize
    # the exact generated, all-TEXT record projection to measure the hypothesis.
    # Never use this exception for user-supplied SQL or the GUI service.
    if any("jsonb_to_record" in s for d in recipes for s in d["statements"]):
        allowed = [prepared(m, [r[0] for r in conn.execute("SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1")])
                   for m in ("record_once", "record_table1", "record_table2")]
        shape = lambda ds: [(d["table_name"], d["statements"]) for d in ds]
        if shape(recipes) not in [shape(ds) for ds in allowed]:
            raise RuntimeError("Noncanonical record projection refused")
        metadata = conn.execute("SELECT n.nspname,p.provolatile,p.prosecdef FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.proname='jsonb_to_record' AND pg_function_is_visible(p.oid)").fetchall()
        if metadata != [("pg_catalog", "s", False)]:
            raise RuntimeError("Unexpected record function implementation")
        filtered = [{**d, "function_references": [r for r in d["function_references"] if r[1] != "jsonb_to_record"]} for d in recipes]
        service._validate_cross_table_expansion_functions("diagnostic", filtered)
        return False
    service._validate_cross_table_expansion_functions("diagnostic", recipes)
    return True


def execute(conn, mode, scopes, *, profile=False):
    recipes = prepared(mode, scopes)
    guard_parity = validate_functions(conn, recipes)
    conn.execute("TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY")
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS diagnostic_timing(stage text, seconds double precision, rows bigint) ON COMMIT DELETE ROWS")
    stages = [("source_manifest", PostgresAdminService._build_expansion_manifest_sql("public", "raw_data", scopes).split("SELECT json_build_object(")[0])]
    stages += [(d["table_name"], s) for d in recipes for s in d["statements"]]
    start_lsn = conn.execute("SELECT pg_current_wal_insert_lsn()").fetchone()[0]
    sample = {"backend_rss_peak_bytes": 0, "application_rss_peak_bytes": 0}
    stop = threading.Event()
    pid = conn.info.backend_pid
    def monitor():
        try:
            backend = psutil.Process(pid)  # local server only
            while not stop.is_set():
                sample["backend_rss_peak_bytes"] = max(sample["backend_rss_peak_bytes"], backend.memory_info().rss)
                sample["application_rss_peak_bytes"] = max(sample["application_rss_peak_bytes"], psutil.Process().memory_info().rss)
                stop.wait(0.05)
        except (psutil.Error, OSError):
            sample["backend_rss_peak_bytes"] = None
    watcher = threading.Thread(target=monitor, daemon=True)
    watcher.start()
    try:
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            conn.execute(PostgresAdminService._build_cross_table_expansion_lock_sql(recipes))
            for name, statement in stages:
                if profile and name != "source_manifest":
                    plan = conn.execute("EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, SUMMARY, TIMING OFF, FORMAT JSON) " + statement).fetchone()[0]
                    write_report(ROOT / f"reports/plans/{mode}_{name}.json", plan)
                else:
                    body = statement.strip().rstrip(";")
                    conn.execute("DO $diag$ DECLARE started timestamptz := clock_timestamp(); affected bigint; BEGIN " + body +
                        "; GET DIAGNOSTICS affected = ROW_COUNT; INSERT INTO diagnostic_timing VALUES ('" + name + "', EXTRACT(EPOCH FROM clock_timestamp()-started), affected); END $diag$;")
            timing = conn.execute("SELECT stage,seconds,rows FROM diagnostic_timing").fetchall()
            conn.execute("SELECT pg_stat_force_next_flush()")
            before_commit = time.perf_counter()
        commit_seconds = time.perf_counter() - before_commit
    finally:
        stop.set()
        watcher.join(timeout=1)
    wal = conn.execute("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s)::bigint", (start_lsn,)).fetchone()[0]
    stats = conn.execute("SELECT temp_files,temp_bytes FROM pg_stat_database WHERE datname=current_database()").fetchone()
    return {"mode": mode, "official_function_guard_parity": guard_parity, "sql_seconds": sum(t[1] for t in timing), "commit_client_wall_seconds": commit_seconds,
            "stages": [{"name": n, "seconds": s, "rows": r} for n,s,r in timing],
            "wal_cluster_delta_bytes": wal, "db_cumulative_temp_files": stats[0], "db_cumulative_temp_bytes": stats[1], **sample}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="lab_100k")
    parser.add_argument("--modes", nargs="+", choices=MODES, default=["baseline", "payload_once", "lazy_keys", "record_once"])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--jit", choices=["on", "off"])
    parser.add_argument("--work-mem")
    parser.add_argument("--output", default="reports/sql-benchmark.json")
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("repeats must be positive")
    with connect(args.database, autocommit=True) as conn:
        require_lab(conn)
        if args.jit:
            conn.execute("SELECT set_config('jit',%s,false)", (args.jit,))
        if args.work_mem:
            conn.execute("SELECT set_config('work_mem',%s,false)", (args.work_mem,))
        scopes = [r[0] for r in conn.execute("SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1")]
        report = {"kind": "diagnostic_sql_only", "official_score": False, "eligible_submission": False,
                  "timestamp_utc": datetime.now(timezone.utc).isoformat(), "environment": environment(conn),
                  "source_rows": conn.execute("SELECT count(*) FROM public.raw_data").fetchone()[0],
                  "exclusions": ["full preflight", "managed row counters", "version registration", "dump handling", "GUI", "SSH"],
                  "resource_limits": "50ms RSS samples of local backend/application; shared mappings included; no global peak; WAL cluster delta includes background activity", "runs": []}
        schema = schema_signature(conn)
        print("Creating independent official SQL reference / warmup...", flush=True)
        execute(conn, "baseline", scopes)
        snapshot(conn)
        reference = fingerprints(conn)
        checked = set()
        for repeat in range(args.repeats):
            modes = args.modes if repeat % 2 == 0 else list(reversed(args.modes))
            for mode in modes:
                result = execute(conn, mode, scopes, profile=args.profile)
                if schema_signature(conn) != schema:
                    raise AssertionError("Schema/constraints/indexes changed")
                assert_relationships(conn)
                if fingerprints(conn) != reference:
                    raise AssertionError(f"Typed fingerprints differ: {mode}")
                if mode not in checked:
                    assert_exact(conn)
                    checked.add(mode)
                    result["exact_except_all"] = True
                result.update(repeat=repeat, fingerprints_equivalent=True, schema_equivalent=True)
                result["recipe_sha256"] = hashlib.sha256("\n".join(d["sql"] for d in destinations(mode)).encode()).hexdigest()
                report["runs"].append(result)
                write_report(args.output, report)
                print(f"{repeat+1}: {mode}: {result['sql_seconds']:.4f}s; six outputs equivalent", flush=True)
        report["summary"] = {}
        baseline = statistics.median(r["sql_seconds"] for r in report["runs"] if r["mode"] == "baseline") if "baseline" in args.modes else None
        for mode in args.modes:
            values = [r["sql_seconds"] for r in report["runs"] if r["mode"] == mode]
            median = statistics.median(values)
            report["summary"][mode] = {"individual_seconds": values, "median": median, "min": min(values), "max": max(values),
                "reduction_percent": 100*(1-median/baseline) if baseline else None}
        report["database_bytes"] = conn.execute("SELECT pg_database_size(current_database())").fetchone()[0]
        write_report(args.output, report)
        print(report["summary"], flush=True)


if __name__ == "__main__":
    main()
