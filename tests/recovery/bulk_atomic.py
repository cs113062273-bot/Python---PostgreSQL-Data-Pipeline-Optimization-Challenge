"""Rollback bulk DDL/data at each boundary, including a killed backend.

Uses only a newly generated disposable fixture. This tests the atomic strategy;
the independently tested chunk protocol retains its own checkpoint behavior.
"""
import argparse
import uuid
import psycopg
from bench.common import connect, write_report, require_lab
from bench.fixture import create_fixture
from bench.run import prepared
from pipeline.guarded_normalization import MODE as GUARDED_MODE, MODES as GUARDED_MODES, prepare_sql as prepare_guarded, fast_statement as guarded_statement
from pipeline.normalization import MODES as NORMALIZATION_MODES, prepare_sql as normalize_sql, rewrite as normalized_statement
from pipeline.recipes import ORDER, bulk_unique_tables, BULK_MODES
from pipeline.bulk_fks import prepare_sql, restore_sql
from pipeline.bulk_indexes import prepare_mode_sql as prepare_index, restore_sql as restore_index
from tests.equivalence.oracle import schema_signature, fingerprints


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode",choices=BULK_MODES,default="bulk_payload")
    parser.add_argument("--output",default="reports/recovery-bulk-atomic.json")
    args=parser.parse_args()
    database="lab_bulk_atomic_"+uuid.uuid4().hex[:8]
    create_fixture(database)
    with connect(database,autocommit=True) as observer:
        require_lab(observer)
        before=schema_signature(observer)
        empty=fingerprints(observer)
        scopes=[r[0] for r in observer.execute("SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1")]
        recipes=prepared(args.mode,scopes)
        tests=[]
        points=["after_fk_drop"]
        if args.mode in NORMALIZATION_MODES or args.mode in GUARDED_MODES:points.append("after_normalization")
        for table in ORDER:
            if table in bulk_unique_tables(args.mode):points.append(f"after_{table}_index_drop")
            points.append(f"after_{table}_insert")
            if table in bulk_unique_tables(args.mode):points.append(f"after_{table}_index_restore")
        points.extend(["before_fk_restore","after_fk_restore"])
        for point in points:
            for kill in (False,True):
                victim=connect(database,autocommit=True)
                hit=False
                def inject(event):
                    nonlocal hit
                    if event!=point:return
                    hit=True
                    if kill:
                        assert observer.execute("SELECT pg_terminate_backend(%s)",(victim.info.backend_pid,)).fetchone()[0]
                        victim.execute("SELECT 1")
                    raise RuntimeError("injected failure")
                try:
                    with victim.transaction():
                        victim.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                        if args.mode in GUARDED_MODES:
                            victim.execute(prepare_guarded(scopes,args.mode));inject("after_normalization")
                            assert victim.execute("SELECT safe FROM pg_temp.pipeline_normalization_domain").fetchone()[0]
                        victim.execute(prepare_sql());inject("after_fk_drop")
                        for d in recipes:
                            table=d["pure_table_name"]
                            if args.mode in NORMALIZATION_MODES and table=="table1":victim.execute(normalize_sql(scopes));inject("after_normalization")
                            if table in bulk_unique_tables(args.mode):
                                victim.execute(prepare_index(args.mode,table));inject(f"after_{table}_index_drop")
                            for statement in d["statements"]:victim.execute(guarded_statement(statement,scopes,args.mode) if args.mode in GUARDED_MODES else normalized_statement(statement,scopes) if args.mode in NORMALIZATION_MODES else statement)
                            if args.mode=='guarded_prefix' and table in ('table3','table4'):victim.execute('ANALYZE public.'+table)
                            inject(f"after_{table}_insert")
                            if table in bulk_unique_tables(args.mode):
                                victim.execute(restore_index(table));inject(f"after_{table}_index_restore")
                        inject("before_fk_restore")
                        victim.execute(restore_sql());inject("after_fk_restore")
                except (RuntimeError,psycopg.Error):
                    if not hit:raise
                finally:victim.close()
                assert hit,point
                assert schema_signature(observer)==before,(point,"schema")
                assert fingerprints(observer)==empty,(point,"data")
                assert observer.execute("SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace AND NOT convalidated").fetchone()[0]==0
                tests.append({"boundary":point,"failure":"backend_termination" if kill else "exception","passed":True})
                print(point,"backend termination" if kill else "exception",": PASS",flush=True)
        write_report(args.output,{"kind":"atomic_bulk_ddl_data_rollback","database":database,"mode":args.mode,
            "tests":tests,"all_passed":True,"scope":"All six destination contents and exact constraints/index schema restored; identity sequences may contain ordinary rollback gaps"})


if __name__=="__main__":main()
