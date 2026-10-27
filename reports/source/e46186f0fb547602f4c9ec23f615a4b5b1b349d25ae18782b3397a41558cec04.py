"""A target commit during lock wait must be visible to the guarded proof."""
import argparse
import threading
import time
import uuid
from bench.common import connect,write_report
from bench.fixture import create_fixture
from bench.control import seed_initial_versions
from pipeline.recipes import ORDER,destinations,bulk_unique_tables
from tests.recovery.bulk_server_crash import Service as PrefixService


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode',choices=['guarded_prefix','guarded_ctas','guarded_copy'],default='guarded_prefix')
    parser.add_argument('--output',default='reports/recovery-guarded-concurrency.json')
    args=parser.parse_args()
    class Service(PrefixService):
        bulk_recipe_mode=args.mode
        bulk_unique_tables=bulk_unique_tables(args.mode)
    database='lab_bulk_concurrency_'+uuid.uuid4().hex[:8]
    create_fixture(database);seed_initial_versions(database)
    service=Service();service.ensure_data_database_metadata(database)
    for table in ('raw_data',)+ORDER:service.get_table_row_count(database,'public.'+table)
    result=[];errors=[]
    def run():
        try:result.append(service.execute_cross_table_expansion(database,'public.raw_data',['group001'],destinations(args.mode)))
        except BaseException as error:errors.append(error)
    with connect(database,autocommit=True) as writer,connect(database,autocommit=True) as observer:
        writer.execute('BEGIN; LOCK TABLE public.table1 IN ROW EXCLUSIVE MODE;')
        writer.execute("INSERT INTO public.table3(text_column_1,text_column_2) VALUES('CONCURRENT-SENTINEL','INDEPENDENT')")
        worker=threading.Thread(target=run,daemon=True);worker.start()
        started=time.monotonic();blocked=None
        try:
            while time.monotonic()-started<60:
                blocked=observer.execute("SELECT pid,backend_xmin::text FROM pg_stat_activity WHERE datname=%s AND application_name='psql' AND wait_event_type='Lock' AND query LIKE 'LOCK TABLE%%' AND position('table6' in query)>0",(database,)).fetchone()
                if blocked:break
                if errors:raise errors[0]
                time.sleep(.05)
            assert blocked,'Protected target lock wait was not observed'
            # The pipeline already completed preflight, but cannot take its
            # protected data snapshot until this concurrent writer commits.
            writer.execute("UPDATE public.pgdm_table_row_counts SET row_count=row_count+1 WHERE schema_name='public' AND table_name='table3'")
            writer.execute('COMMIT')
        finally:
            if writer.info.transaction_status.name!='IDLE':writer.execute('ROLLBACK')
        worker.join(timeout=60)
        assert not worker.is_alive() and not errors,errors
        assert result[0]['execution_architecture']['safe_domain'] is False
        if args.mode=='guarded_copy':assert result[0]['execution_architecture']['copy_workspace']['cleanup_success']
        assert observer.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()[0] is None
        assert observer.execute("SELECT count(*) FROM public.table3 WHERE text_column_1='CONCURRENT-SENTINEL'").fetchone()[0]==1
        assert observer.execute('SELECT count(*) FROM public.table1').fetchone()[0]==64
        for table in ORDER:
            actual=observer.execute(f'SELECT count(*) FROM public.{table}').fetchone()[0]
            cached=observer.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s",(table,)).fetchone()[0]
            assert actual==cached,(table,actual,cached)
    write_report(args.output,{'kind':'concurrent_target_commit_during_lock_wait','database':database,'mode':args.mode,
        'passed':True,'proof_fell_back':True,'concurrent_row_preserved':True,'counter_reconciliation':True,'blocked_backend_xmin':blocked[1]})
    print('Concurrent target commit visible after lock wait; original fallback, data and counters: PASS',flush=True)


if __name__=='__main__':main()
