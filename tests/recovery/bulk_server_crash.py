"""Immediate stop of the verified private cluster during uncommitted bulk DDL.

After WAL recovery, verify exact schema/data/counters, then retry through the
original service and register all six versions. Run without other active jobs.
"""
import argparse
from pathlib import Path
import subprocess
import threading
import time
import uuid
import psycopg
from bench.common import ROOT,connect,write_report
from bench.fixture import create_fixture
from bench.control import seed_initial_versions
from bench.run import prepared,execute
from pipeline.bulk_fks import BulkFKMixin,prepare_sql as prepare_fks,restore_sql as restore_fks
from pipeline.bulk_indexes import BulkIndexMixin,prepare_mode_sql,restore_sql as restore_indexes
from pipeline.guarded_normalization import GuardedNormalizationMixin,prepare_sql,insert_sql
from pipeline.planner import Table1PlannerMixin
from pipeline.local_service import LocalService
from pipeline.recipes import ORDER,destinations,bulk_unique_tables
from pipeline.upstream import PostgresAdminService
from pipeline.checkpoints import source_digest
from tests.equivalence.oracle import schema_signature,fingerprints,snapshot,assert_exact,assert_relationships


MODE='guarded_prefix'


class Service(BulkFKMixin,BulkIndexMixin,GuardedNormalizationMixin,Table1PlannerMixin,LocalService):
    bulk_foreign_keys=True
    bulk_recipe_mode=MODE
    bulk_unique_tables=bulk_unique_tables(MODE)
    guarded_normalization=True
    capture_batch_sql=True
    table1_hash_join=False


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pg-ctl',required=True)
    parser.add_argument('--data-dir',required=True)
    parser.add_argument('--port',type=int,default=55432)
    args=parser.parse_args()
    directory=Path(args.data_dir).resolve();runtime=(ROOT/'.runtime').resolve()
    if runtime not in directory.parents or (directory/'PG_VERSION').read_text().strip()!='17':
        parser.error('Only the verified project-private PostgreSQL 17 cluster may be stopped')
    with connect('postgres',autocommit=True) as admin:
        assert admin.info.host in ('127.0.0.1','localhost','::1')
        assert Path(admin.execute('SHOW data_directory').fetchone()[0]).resolve()==directory
        assert admin.execute('SELECT inet_server_port()').fetchone()[0]==args.port
        assert not admin.execute("SELECT 1 FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Another active database job exists'
    database='lab_atomic_crash_'+uuid.uuid4().hex[:8]
    create_fixture(database);seed_initial_versions(database)
    service=Service();scopes=['group001']
    with connect(database,autocommit=True) as conn:
        execute(conn,'baseline',scopes);snapshot(conn)
        conn.execute('TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY')
    service.ensure_data_database_metadata(database)
    for table in ('raw_data',)+ORDER:service.get_table_row_count(database,'public.'+table)
    with connect(database,autocommit=True) as conn:
        before_schema=schema_signature(conn);before_empty=fingerprints(conn);before_source=source_digest(conn,scopes)
        before_counters=conn.execute('SELECT to_jsonb(c) FROM public.pgdm_table_row_counts c ORDER BY schema_name,table_name').fetchall()
    ready=threading.Event();errors=[]
    def worker():
        try:
            with connect(database,autocommit=True) as victim:
                victim.execute('BEGIN ISOLATION LEVEL REPEATABLE READ; LOCK TABLE public.raw_data IN SHARE MODE;')
                victim.execute(prepare_sql(scopes,MODE));victim.execute(prepare_fks())
                for d in prepared(MODE,scopes):
                    table=d['pure_table_name']
                    if table in bulk_unique_tables(MODE):victim.execute(prepare_mode_sql(MODE,table))
                    for statement in d['statements']:
                        victim.execute(PostgresAdminService._build_managed_dml_sql(insert_sql(statement,scopes,MODE),'public',table,'add'))
                    if table in bulk_unique_tables(MODE):victim.execute(restore_indexes(table))
                ready.set()
                victim.execute('SELECT pg_sleep(600)')
                victim.execute(restore_fks());victim.execute('COMMIT')
        except BaseException as error:errors.append((type(error).__name__,str(error)))
    thread=threading.Thread(target=worker,daemon=True);thread.start()
    if not ready.wait(90):raise RuntimeError(('Crash boundary not reached',errors))
    print('All six managed inserts uncommitted, FKs not yet restored; immediate server stop',flush=True)
    subprocess.run([args.pg_ctl,'-D',str(directory),'-m','immediate','-w','stop'],check=True)
    started=time.monotonic()
    subprocess.run([args.pg_ctl,'-D',str(directory),'-l',str(runtime/'postgres.log'),'-o',f'-h 127.0.0.1 -p {args.port}','-W','start'],check=True)
    while True:
        try:
            with connect('postgres',autocommit=True,connect_timeout=1) as conn:conn.execute('SELECT 1')
            break
        except psycopg.OperationalError:
            if time.monotonic()-started>600:raise RuntimeError('Recovery did not finish within 10 minutes')
            time.sleep(2)
    thread.join(timeout=10)
    assert not thread.is_alive() and errors,errors
    with connect(database,autocommit=True) as conn:
        assert schema_signature(conn)==before_schema
        assert fingerprints(conn)==before_empty
        assert source_digest(conn,scopes)==before_source
        assert conn.execute('SELECT to_jsonb(c) FROM public.pgdm_table_row_counts c ORDER BY schema_name,table_name').fetchall()==before_counters
    print('WAL recovery restored exact original schema, empty destinations and counters; retrying original service',flush=True)
    result=service.execute_cross_table_expansion(database,'public.raw_data',scopes,destinations(MODE))
    versions=service.register_cross_table_expansion_versions(result,'research','local-crash-test')
    with connect(database,autocommit=True) as conn:
        assert_exact(conn);assert_relationships(conn)
        for table in ORDER:
            assert conn.execute(f'SELECT count(*) FROM public.{table}').fetchone()[0]==conn.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s",(table,)).fetchone()[0]
    assert len(versions)==6
    write_report('reports/recovery-guarded-server-crash.json',{'kind':'actual_private_cluster_immediate_stop_during_guarded_bulk','database':database,
        'passed':True,'restart_seconds':time.monotonic()-started,'uncommitted_schema_data_counters_rolled_back':True,
        'retry_exact_six_tables':True,'retry_counter_reconciliation':True,'versions':versions,
        'scope':'pre-commit server crash and full retry; not power-loss or post-commit version-saga recovery'})
    print('Guarded atomic server crash / full native-service retry: PASS',flush=True)


if __name__=='__main__':main()
