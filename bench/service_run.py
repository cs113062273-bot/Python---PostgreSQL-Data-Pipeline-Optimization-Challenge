"""Full organizer service integration over LOCAL psql, without SSH/GUI parity."""
import argparse
from datetime import datetime,timezone
import time

from bench.common import connect,environment,require_lab,write_report
from bench.control import seed_initial_versions
from bench.run import execute
from pipeline.local_service import LocalService
from pipeline.planner import Table1PlannerMixin
from pipeline.bulk_fks import BulkFKMixin
from pipeline.bulk_indexes import BulkIndexMixin
from pipeline.normalization import NormalizationMixin
from pipeline.guarded_normalization import GuardedNormalizationMixin, MODES as GUARDED_MODES
from pipeline.recipes import BULK_MODES, bulk_unique_tables
from pipeline.recipes import destinations
from pipeline.upstream import PostgresAdminService
from tests.equivalence.oracle import snapshot,assert_exact,assert_relationships
from ui.dialogs import ExpansionTimingReportDialog


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database",default="lab_full_100k")
    parser.add_argument("--mode",default="baseline",choices=["baseline","path_table1","path_array","qf_lazy","qf_lazy_path1","hash_table1","hash_array","qf_hash1","hash_array64","hash_array64_nomemo","bulk_fks","bulk_index2","bulk_indexes","bulk_index_payload","bulk_payload","bulk_array","bulk_full","bulk_full64","bulk_normalize","bulk_normalize64","guarded_normalize","guarded_prefix","guarded_parallel"])
    parser.add_argument("--output",default="reports/local-service.json")
    parser.add_argument("--reference",help="Separate existing oracle; avoids all destination warmup in the measured database")
    args=parser.parse_args()
    with connect(args.database,autocommit=True) as conn:
        require_lab(conn)
        env=environment(conn)
        scopes=[r[0] for r in conn.execute("SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1")]
        if conn.execute("SELECT to_regclass('public.pgdm_table_row_counts')").fetchone()[0]:
            raise RuntimeError("Use a fresh lab clone so metadata/version history remains coherent")
        if not args.reference:
            execute(conn,"baseline",scopes)
            snapshot(conn)
            conn.execute("TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY")
    seed_initial_versions(args.database)
    class Service(BulkFKMixin,BulkIndexMixin,GuardedNormalizationMixin,NormalizationMixin,Table1PlannerMixin,LocalService):
        capture_batch_sql=True
        bulk_foreign_keys=args.mode in BULK_MODES
        guarded_normalization=args.mode in GUARDED_MODES
        normalize_input=args.mode in ("bulk_normalize","bulk_normalize64")
        bulk_recipe_mode=args.mode
        bulk_unique_tables=bulk_unique_tables(args.mode)
        table1_hash_join=args.mode in ("hash_table1","hash_array","qf_hash1","hash_array64","hash_array64_nomemo")
        table2_memoize=args.mode!="hash_array64_nomemo"
        expansion_work_mem="64MB" if args.mode in ("hash_array64","hash_array64_nomemo","bulk_full64","bulk_normalize64") else None
    service=Service()
    report=PostgresAdminService.create_cross_table_expansion_timing_report(args.database,"public.raw_data",scopes,[d["table_name"] for d in destinations()])
    started=time.perf_counter()
    result=service.execute_cross_table_expansion(args.database,"public.raw_data",scopes,destinations(args.mode),timing_report=report)
    versions=service.register_cross_table_expansion_versions(result,"research","local-diagnostic",timing_report=report)
    wall=time.perf_counter()-started
    if args.mode in GUARDED_MODES:
        import os
        with connect(os.environ.get('PDM_CONTROL_DB','postgres_data_manager'),autocommit=True) as control:
            history=control.execute("SELECT sql_recipe FROM app_control.table_versions WHERE database_name=%s AND version_code<>'0000'",(args.database,)).fetchall()
            assert len(history)==6
            for recipe, in history:
                assert 'compiler_sha256='+result['execution_architecture']['compiler_sha256'] in recipe
                assert 'batch_sql_sha256='+result['execution_architecture']['batch_sql_sha256'] in recipe
    report["completed_at"]=datetime.now(timezone.utc).isoformat()
    with connect(args.database,autocommit=True) as conn:
        if args.reference:
            from bench.fresh import exact_to_reference
            with connect(args.reference,autocommit=True) as reference:
                require_lab(reference)
                exact_to_reference(conn,reference)
        else:
            assert_exact(conn)
        assert_relationships(conn)
        for table in ("table3","table4","table1","table5","table6","table2"):
            count=conn.execute(f"SELECT count(*) FROM public.{table}").fetchone()[0]
            counter=conn.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s",(table,)).fetchone()[0]
            assert count==counter
    value={"kind":"local_transport_organizer_service","official_score":False,"environment":env,"mode":args.mode,
           "execution_architecture":result.get('execution_architecture'),
           "separate_reference":args.reference,"destination_warmup_before_measured_run":not bool(args.reference),
           "wall_seconds":wall,"exact_six_tables":True,"counter_reconciliation":True,"versions":versions,"timing_report":report,
           "limitations":["local Windows psql substitutes Linux/SSH command transport","GUI cache/refresh excluded","dump filesystem/replay not exercised by this command"]}
    write_report(args.output,value)
    from pathlib import Path
    Path(args.output).with_suffix(".txt").write_text("LOCAL TRANSPORT DIAGNOSTIC; NOT AN OFFICIAL BASELINE OR SCORE\n\n"+ExpansionTimingReportDialog.build_report_text(report),encoding="utf-8")
    print(f"Local service {args.mode}: {wall:.3f}s wall; all six outputs, counters and versions verified",flush=True)


if __name__=="__main__":main()
