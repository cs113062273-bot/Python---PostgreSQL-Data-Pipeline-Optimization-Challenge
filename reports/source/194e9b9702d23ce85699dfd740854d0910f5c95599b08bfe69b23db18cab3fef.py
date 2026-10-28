"""Immediate private-server crash during the actual native psql COPY batch."""
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
from bench.run import execute
from pipeline.bulk_fks import BulkFKMixin
from pipeline.bulk_indexes import BulkIndexMixin
from pipeline.guarded_normalization import GuardedNormalizationMixin
from pipeline.local_service import LocalService
from pipeline.recipes import ORDER,destinations,bulk_unique_tables
from pipeline.upstream import PostgresAdminService
from tests.recovery.native_copy_atomic import source_sha
from tests.equivalence.oracle import protected_schema_signature,sequence_signature,fingerprints,snapshot,assert_exact,assert_relationships


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--pg-ctl',required=True)
    p.add_argument('--data-dir',required=True)
    p.add_argument('--port',type=int,default=55432)
    p.add_argument('--startup-log',help='Optional project-private recovery log')
    p.add_argument('--wal-level',choices=['minimal','replica'],help='Preserve the explicit research WAL setting across restart')
    p.add_argument('--fresh-service-retry',action='store_true',help='Model a restarted processing worker with fresh connection state after server recovery')
    args=p.parse_args()
    directory=Path(args.data_dir).resolve();runtime=(ROOT/'.runtime').resolve()
    startup_log=Path(args.startup_log).resolve() if args.startup_log else runtime/'postgres.log'
    if not startup_log.is_relative_to(runtime):p.error('Recovery log must stay in the private runtime')
    if runtime not in directory.parents or (directory/'PG_VERSION').read_text().strip()!='17':p.error('Only this private PostgreSQL 17 data directory may be stopped')
    with connect('postgres',autocommit=True) as c:
        assert c.info.host in ('localhost','127.0.0.1','::1')
        assert Path(c.execute('SHOW data_directory').fetchone()[0]).resolve()==directory
        assert c.execute('SELECT inet_server_port()').fetchone()[0]==args.port
        if args.wal_level:assert c.execute('SHOW wal_level').fetchone()==(args.wal_level,)
        assert not c.execute("SELECT 1 FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Another active job exists'
    db='lab_native_crash_'+uuid.uuid4().hex[:8]
    create_fixture(db);seed_initial_versions(db)
    tag='native_copy_crash_'+uuid.uuid4().hex
    class PausedLocal(LocalService):
        pause=False
        def run_remote_command(self,command,stdin_text=None,**kwargs):
            if self.pause and isinstance(stdin_text,str) and 'BEGIN ISOLATION LEVEL REPEATABLE READ;' in stdin_text and 'pgdm_cross_table_hash_manifest' in stdin_text:
                marker="INSERT INTO pg_temp.pgdm_expansion_timings VALUES\n      (8000,'bulk_load'"
                assert stdin_text.count(marker)==1
                stdin_text="SET application_name='"+tag+"';\n"+stdin_text.replace(marker,'SELECT pg_sleep(600);\n'+marker,1)
            return super().run_remote_command(command,stdin_text=stdin_text,**kwargs)
    class Service(BulkFKMixin,BulkIndexMixin,GuardedNormalizationMixin,PausedLocal):
        bulk_foreign_keys=guarded_normalization=True
        bulk_recipe_mode='guarded_copy'
        bulk_unique_tables=bulk_unique_tables('guarded_copy')
        capture_batch_sql=True
    service=Service()
    with connect(db,autocommit=True) as c:
        execute(c,'baseline',['group001']);snapshot(c)
        c.execute('TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY')
    service.ensure_data_database_metadata(db)
    for table in ('raw_data',)+ORDER:service.get_table_row_count(db,'public.'+table)
    with connect(db,autocommit=True) as c:
        before_schema=protected_schema_signature(c);before_sequences=sequence_signature(c);before_empty=fingerprints(c);before_source=source_sha(c)
        before_counters=c.execute('SELECT to_jsonb(c) FROM public.pgdm_table_row_counts c ORDER BY schema_name,table_name').fetchall()
    outcome={}
    def worker():
        try:
            service.pause=True
            report=PostgresAdminService.create_cross_table_expansion_timing_report(db,'public.raw_data',['group001'],[d['table_name'] for d in destinations()])
            service.execute_cross_table_expansion(db,'public.raw_data',['group001'],destinations('guarded_copy'),timing_report=report)
        except Exception as error:outcome['error']=str(error)
        finally:outcome['copy_cleanup']=getattr(service._expansion_timing_local,'copy_workspace_result',None)
    thread=threading.Thread(target=worker,daemon=True);thread.start()
    deadline=time.monotonic()+90
    with connect('postgres',autocommit=True) as observer:
        while time.monotonic()<deadline:
            row=observer.execute("SELECT pid FROM pg_stat_activity WHERE datname=%s AND application_name=%s AND state='active' AND query LIKE 'SELECT pg_sleep%%'",(db,tag)).fetchone()
            if row:break
            if not thread.is_alive():raise RuntimeError(outcome)
            time.sleep(0.05)
        else:raise RuntimeError('Native COPY crash boundary was not reached')
    print('Actual COPY data/counters uncommitted, six FKs absent; immediate private-server stop',flush=True)
    subprocess.run([args.pg_ctl,'-D',str(directory),'-m','immediate','-w','stop'],check=True)
    restart=time.monotonic()
    options=f'-h 127.0.0.1 -p {args.port}'
    if args.wal_level:options+=f' -c wal_level={args.wal_level} -c max_wal_senders={0 if args.wal_level=="minimal" else 10}'
    subprocess.run([args.pg_ctl,'-D',str(directory),'-l',str(startup_log),'-o',options,'-W','start'],check=True)
    last_status=restart
    while True:
        try:
            with connect('postgres',autocommit=True,connect_timeout=1) as c:c.execute('SELECT 1')
            break
        except psycopg.OperationalError:
            if time.monotonic()-restart>600:raise RuntimeError('Private WAL recovery did not finish within ten minutes')
            if time.monotonic()-last_status>=30:
                print('Private PostgreSQL startup/recovery remains live; elapsed',round(time.monotonic()-restart),'seconds',flush=True)
                last_status=time.monotonic()
            time.sleep(2)
    thread.join(15)
    assert not thread.is_alive() and outcome.get('error')
    assert outcome['copy_cleanup']['cleanup_success'],outcome
    with connect(db,autocommit=True) as c:
        if args.wal_level:assert c.execute('SHOW wal_level').fetchone()==(args.wal_level,)
        assert protected_schema_signature(c)==before_schema
        assert sequence_signature(c)==before_sequences
        assert fingerprints(c)==before_empty and source_sha(c)==before_source
        assert c.execute('SELECT to_jsonb(c) FROM public.pgdm_table_row_counts c ORDER BY schema_name,table_name').fetchall()==before_counters
        assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()[0] is None
    print('WAL recovery restored exact native COPY rollback; retrying full service and six versions',flush=True)
    if args.fresh_service_retry:
        # Reap the test coordinator's now-dead metadata clients too. The failed
        # worker already cleaned its own thread-local clients. A restarted
        # processing worker never inherits either cache.
        from bench.fast_transport_probe import close_sessions
        close_sessions(service)
        service=Service()
    service.pause=False
    report=PostgresAdminService.create_cross_table_expansion_timing_report(db,'public.raw_data',['group001'],[d['table_name'] for d in destinations()])
    result=service.execute_cross_table_expansion(db,'public.raw_data',['group001'],destinations('guarded_copy'),timing_report=report)
    versions=service.register_cross_table_expansion_versions(result,'research','native-copy-crash-retry',timing_report=report)
    assert len(versions)==6 and result['execution_architecture']['copy_workspace']['cleanup_success']
    with connect(db,autocommit=True) as c:
        assert_exact(c);assert_relationships(c)
        assert sequence_signature(c)==before_sequences and source_sha(c)==before_source
        for table in ORDER:assert c.execute('SELECT count(*) FROM public.'+table).fetchone()[0]==c.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s",(table,)).fetchone()[0]
    write_report('reports/recovery-native-copy-server-crash.json',{'kind':'actual_private_server_immediate_stop_during_native_copy','database':db,'passed':True,'restart_seconds':time.monotonic()-restart,'rollback_schema_sequence_source_counters':True,'copy_files_removed_after_crash':True,'native_retry_exact_six_tables':True,'versions':versions,'execution_architecture':result['execution_architecture'],'local_backend_diagnostics':getattr(service._expansion_timing_local,'local_backend_diagnostics',None),'fresh_service_retry':args.fresh_service_retry,'scope':'Actual native COPY/pre-commit server crash/WAL recovery/full service retry; startup spans the whole retained research cluster, not a scored recovery duration or post-commit control saga.'})
    print('Actual native COPY server crash and complete native retry: PASS',flush=True)


if __name__=='__main__':main()
