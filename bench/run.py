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
from pipeline.guarded_normalization import MODE as GUARDED_MODE, prepare_sql as prepare_guarded, conditional_sql, insert_sql as guarded_insert, fast_statement as guarded_statement, validation_destinations
from pipeline.normalization import MODES as NORMALIZATION_MODES, prepare_sql as normalize_sql, rewrite as normalized_statement
from pipeline.recipes import MODES, ORDER, destinations, BULK_MODES, bulk_unique_tables
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


def execute(conn, mode, scopes, *, profile=False, reset_destinations=True):
    recipes = prepared(mode, scopes)
    guard_parity = validate_functions(conn, validation_destinations(recipes) if mode==GUARDED_MODE else recipes)
    exact_sql = "\n\n".join(s+";" for d in recipes for s in d["statements"])
    sql_sha256 = hashlib.sha256(exact_sql.encode()).hexdigest()
    sql_path = ROOT / "reports/sql" / (sql_sha256+".sql")
    sql_path.parent.mkdir(parents=True,exist_ok=True)
    sql_path.write_text(exact_sql,encoding="utf-8")
    strategy_sha256=None
    if mode in BULK_MODES:
        from pipeline.bulk_fks import prepare_sql as fk_prepare, restore_sql as fk_restore
        from pipeline.bulk_indexes import prepare_mode_sql,restore_sql as index_restore
        # Preserve the actual architecture SQL as well as the six recipe SQLs.
        strategy=["BEGIN ISOLATION LEVEL REPEATABLE READ;","LOCK TABLE public.raw_data IN SHARE MODE;",
                  PostgresAdminService._build_cross_table_expansion_lock_sql(recipes)]
        if mode==GUARDED_MODE:strategy.append(prepare_guarded(scopes))
        strategy.append(conditional_sql(fk_prepare()) if mode==GUARDED_MODE else fk_prepare())
        if mode in ("bulk_full64","bulk_normalize64"):strategy.insert(2,"SET LOCAL work_mem='64MB';")
        strategy.append(PostgresAdminService._build_expansion_manifest_sql("public","raw_data",scopes).split("SELECT json_build_object(")[0])
        for d in recipes:
            table=d["pure_table_name"]
            if table in bulk_unique_tables(mode):strategy.append(conditional_sql(prepare_mode_sql(mode,table)) if mode==GUARDED_MODE else prepare_mode_sql(mode,table))
            if mode in NORMALIZATION_MODES and table=="table1":strategy.append(normalize_sql(scopes))
            strategy.extend(("DO $guarded_insert$ BEGIN\n"+guarded_insert(s,scopes)+"; END $guarded_insert$;" if mode==GUARDED_MODE else (normalized_statement(s,scopes) if mode in NORMALIZATION_MODES else s)+";") for s in d["statements"])
            if table in bulk_unique_tables(mode):strategy.append(conditional_sql(index_restore(table)) if mode==GUARDED_MODE else index_restore(table))
        strategy.extend([conditional_sql(fk_restore()) if mode==GUARDED_MODE else fk_restore(),"COMMIT;"])
        strategy_sql="\n\n".join(strategy)
        strategy_sha256=hashlib.sha256(strategy_sql.encode()).hexdigest()
        (sql_path.parent/(strategy_sha256+".sql")).write_text(strategy_sql,encoding="utf-8")
    if reset_destinations:
        conn.execute("TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY")
    else:
        for table in ORDER:
            if conn.execute(f"SELECT EXISTS(SELECT 1 FROM public.{table})").fetchone()[0]:
                raise RuntimeError("Fresh diagnostic destination is not empty: "+table)
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS diagnostic_timing(stage text, seconds double precision, rows bigint) ON COMMIT DELETE ROWS")
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS diagnostic_guard(safe boolean) ON COMMIT DELETE ROWS")
    stages = [("source_manifest", PostgresAdminService._build_expansion_manifest_sql("public", "raw_data", scopes).split("SELECT json_build_object(")[0])]
    stages += [(d["table_name"], s) for d in recipes for s in d["statements"]]
    start_lsn = conn.execute("SELECT pg_current_wal_insert_lsn()").fetchone()[0]
    sample = {"backend_rss_peak_bytes": 0, "application_rss_peak_bytes": 0}
    stop = threading.Event()
    pid = conn.info.backend_pid
    def monitor():
        try:
            if conn.info.host not in ("127.0.0.1", "localhost", "::1"):
                sample["backend_rss_peak_bytes"] = None
                return
            backend = psutil.Process(pid)  # local server only
            if "postgres" not in backend.name().lower():
                sample["backend_rss_peak_bytes"] = None
                return
            cpu_before=backend.cpu_times()
            while not stop.is_set():
                sample["backend_rss_peak_bytes"] = max(sample["backend_rss_peak_bytes"], backend.memory_info().rss)
                sample["application_rss_peak_bytes"] = max(sample["application_rss_peak_bytes"], psutil.Process().memory_info().rss)
                stop.wait(0.05)
            cpu_after=backend.cpu_times()
            sample['backend_cpu_seconds_including_commit']=cpu_after.user+cpu_after.system-cpu_before.user-cpu_before.system
        except (psutil.Error, OSError):
            sample["backend_rss_peak_bytes"] = None
    watcher = threading.Thread(target=monitor, daemon=True)
    watcher.start()
    try:
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            if mode in BULK_MODES:
                conn.execute("LOCK TABLE public.raw_data IN SHARE MODE")
            if mode in ("hash_array64","hash_array64_nomemo","bulk_full64","bulk_normalize64"):
                if mode in ("bulk_full64","bulk_normalize64"):
                    conn.execute("DO $settings$ DECLARE started timestamptz:=clock_timestamp(); BEGIN PERFORM set_config('work_mem','64MB',true); INSERT INTO diagnostic_timing VALUES('bulk_session_settings',EXTRACT(EPOCH FROM clock_timestamp()-started),NULL); END $settings$;")
                else:conn.execute("SET LOCAL work_mem='64MB'")
            conn.execute(PostgresAdminService._build_cross_table_expansion_lock_sql(recipes))
            guarded_safe=None
            if mode==GUARDED_MODE:
                conn.execute("DO $domain$ DECLARE started timestamptz:=clock_timestamp(); BEGIN "+prepare_guarded(scopes)+" INSERT INTO diagnostic_timing VALUES('normalize_and_prove_domain',EXTRACT(EPOCH FROM clock_timestamp()-started),NULL); END $domain$;")
                guarded_safe=conn.execute("SELECT safe FROM pg_temp.pipeline_normalization_domain").fetchone()[0]
            if mode in BULK_MODES and (mode!=GUARDED_MODE or guarded_safe):
                from pipeline.bulk_fks import prepare_sql,restore_sql
                conn.execute("DO $timer$ DECLARE started timestamptz:=clock_timestamp(); BEGIN "+prepare_sql()+" INSERT INTO diagnostic_timing VALUES('bulk_fk_prepare',EXTRACT(EPOCH FROM clock_timestamp()-started),NULL); END $timer$;")
            for name, statement in stages:
                if mode==GUARDED_MODE and guarded_safe and name!="source_manifest":statement=guarded_statement(statement,scopes)
                if mode in NORMALIZATION_MODES:
                    if name=="public.table1":
                        conn.execute("DO $normalize$ DECLARE started timestamptz:=clock_timestamp(); BEGIN "+normalize_sql(scopes)+" INSERT INTO diagnostic_timing VALUES('normalize_text_input',EXTRACT(EPOCH FROM clock_timestamp()-started),NULL); END $normalize$;")
                    statement=normalized_statement(statement,scopes)
                unique_table=name.removeprefix("public.")
                if unique_table in bulk_unique_tables(mode) and (mode!=GUARDED_MODE or guarded_safe):
                    from pipeline.bulk_indexes import prepare_mode_sql as prepare_index, restore_sql as restore_index
                    conn.execute("DO $timer$ DECLARE started timestamptz:=clock_timestamp(); BEGIN "+prepare_index(mode,unique_table)+" INSERT INTO diagnostic_timing VALUES('bulk_unique_prepare_"+unique_table+"',EXTRACT(EPOCH FROM clock_timestamp()-started),NULL); END $timer$;")
                original_mergejoin=None
                if mode in ("hash_table1","qf_hash1","hash_array","hash_array64","hash_array64_nomemo") and name=="public.table1":
                    original_mergejoin=conn.execute("SHOW enable_mergejoin").fetchone()[0]
                    conn.execute("SET LOCAL enable_mergejoin=off")
                original_memoize=None
                if mode=="hash_array64_nomemo" and name=="public.table2":
                    original_memoize=conn.execute("SHOW enable_memoize").fetchone()[0]
                    conn.execute("SET LOCAL enable_memoize=off")
                if mode=="guarded_qf" and name=="public.table2":
                    from pipeline.guards import gate_sql,fast_statement
                    gate=gate_sql(scopes)
                    conn.execute("DO $gate$ DECLARE started timestamptz:=clock_timestamp(); BEGIN INSERT INTO diagnostic_guard "+gate+"; INSERT INTO diagnostic_timing VALUES ('semantic_domain_gate',EXTRACT(EPOCH FROM clock_timestamp()-started),NULL); END $gate$;")
                    safe=conn.execute("SELECT safe FROM diagnostic_guard").fetchone()[0]
                    if safe:
                        statement=fast_statement(tuple(scopes))
                if profile and name != "source_manifest":
                    plan = conn.execute("EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, SUMMARY, TIMING OFF, FORMAT JSON) " + statement).fetchone()[0]
                    write_report(ROOT / f"reports/plans/{mode}_{name}.json", plan)
                else:
                    body = statement.strip().rstrip(";")
                    conn.execute("DO $diag$ DECLARE started timestamptz := clock_timestamp(); affected bigint; BEGIN " + body +
                        "; GET DIAGNOSTICS affected = ROW_COUNT; INSERT INTO diagnostic_timing VALUES ('" + name + "', EXTRACT(EPOCH FROM clock_timestamp()-started), affected); END $diag$;")
                if unique_table in bulk_unique_tables(mode) and (mode!=GUARDED_MODE or guarded_safe):
                    conn.execute("DO $timer$ DECLARE started timestamptz:=clock_timestamp(); BEGIN "+restore_index(unique_table)+" INSERT INTO diagnostic_timing VALUES('bulk_unique_restore_"+unique_table+"',EXTRACT(EPOCH FROM clock_timestamp()-started),NULL); END $timer$;")
                if original_mergejoin is not None:
                    conn.execute("SELECT set_config('enable_mergejoin',%s,true)",(original_mergejoin,))
                if original_memoize is not None:
                    conn.execute("SELECT set_config('enable_memoize',%s,true)",(original_memoize,))
                maintained=("public.table3","public.table4","public.table1") if mode=="analyze_base" else ("public.table3","public.table4") if mode=="analyze_dims" else ()
                if name in maintained:
                    conn.execute("DO $stats$ DECLARE started timestamptz:=clock_timestamp(); BEGIN ANALYZE "+name+"; INSERT INTO diagnostic_timing VALUES ('analyze_"+name+"',EXTRACT(EPOCH FROM clock_timestamp()-started),NULL); END $stats$;")
            if mode in BULK_MODES and (mode!=GUARDED_MODE or guarded_safe):
                conn.execute("DO $timer$ DECLARE started timestamptz:=clock_timestamp(); BEGIN "+restore_sql()+" INSERT INTO diagnostic_timing VALUES('bulk_fk_restore_validate',EXTRACT(EPOCH FROM clock_timestamp()-started),NULL); END $timer$;")
            timing = conn.execute("SELECT stage,seconds,rows FROM diagnostic_timing").fetchall()
            conn.execute("SELECT pg_stat_force_next_flush()")
            before_commit = time.perf_counter()
        commit_seconds = time.perf_counter() - before_commit
    finally:
        stop.set()
        watcher.join(timeout=1)
    wal = conn.execute("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s)::bigint", (start_lsn,)).fetchone()[0]
    stats = conn.execute("SELECT temp_files,temp_bytes FROM pg_stat_database WHERE datname=current_database()").fetchone()
    if mode=="guarded_qf":
        exact_sql=gate+";\n\n"+"\n\n".join(s+";" for d in recipes[:-1] for s in d["statements"])+"\n\n"+statement+";"
        sql_sha256=hashlib.sha256(exact_sql.encode()).hexdigest()
        (ROOT/"reports/sql"/(sql_sha256+".sql")).write_text(exact_sql,encoding="utf-8")
    return {"mode": mode,"bulk_strategy_sql_sha256":strategy_sha256,"proven_safe_domain":guarded_safe, "table2_memoize_disabled":mode=="hash_array64_nomemo","work_mem_override":"64MB" if mode in ("hash_array64","hash_array64_nomemo","bulk_full64","bulk_normalize64") else None,"table1_mergejoin_disabled":mode in ("hash_table1","qf_hash1","hash_array","hash_array64","hash_array64_nomemo"),"executed_sql_sha256":sql_sha256,"guarded_fast_path":safe if mode=="guarded_qf" else None,"official_function_guard_parity": guard_parity, "sql_seconds": sum(t[1] for t in timing), "commit_client_wall_seconds": commit_seconds,
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
    if args.profile:
        parser.error("Use python -m bench.profile; profiled execution is never ranked as an uninstrumented benchmark")
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
                  "exclusions": ["full preflight", "managed row counters", "version registration", "dump handling", "GUI", "SSH","destination lock acquisition","commit latency (reported separately)"],
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
