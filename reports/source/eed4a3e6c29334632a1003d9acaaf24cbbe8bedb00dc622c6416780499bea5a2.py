"""Native -c versus gexec formats, fresh reads and transaction cleanup."""
import os
import uuid
from bench.common import connect,write_report
from bench.fixture import create_fixture
from pipeline.local_service import LocalService
from bench import fast_transport_probe as probe


def main():
    db='lab_metadata_'+uuid.uuid4().hex[:8];create_fixture(db)
    service=LocalService();probe.install()
    import pipeline.local_diagnostics as diagnostic
    base=[service.psql,'-h',os.environ['PGHOST'],'-p',os.environ['PGPORT'],'-U',os.environ['PGUSER'],'-d',db,'-X','-v','ON_ERROR_STOP=1']
    results=[]
    def run(command,flags=('-qAt',),native=False):
        argv=base+list(flags)+['-c',command]
        if native:
            result=probe.server.ORIGINAL_RUN(argv,None,cwd=None,timeout=20)
            assert not result.returncode,ascii(result.stderr)
            return result.stdout
        token=probe.server.SERVICE.set(service)
        try:return diagnostic.run_psql(argv,None,cwd=None,timeout=20).stdout
        finally:probe.server.SERVICE.reset(token)
    try:
        for flags in (('-qAt',),('-At',),('--csv',),('-q','--csv'),(),('-t',),('-A',)):
            command="SELECT 1 AS n, 'a,'||chr(34)||chr(10)||chr(20013)||chr(25991) AS value UNION ALL SELECT 2,NULL;"
            actual=run(command,flags);expected=run(command,flags,native=True)
            assert actual==expected,(flags,ascii(actual),ascii(expected))
            results.append({'name':'native_output_format','flags':flags,'passed':True})
        if os.name=='nt':
            argv=base+['-qAt','-c',"SELECT '中文';"]
            assert not probe.eligible(argv,None,None)
            results.append({'name':'non_ascii_command_preserves_original_windows_transport','passed':True})
        with connect(db,autocommit=True) as c:
            c.execute('CREATE TABLE public.metadata_probe(v integer); INSERT INTO public.metadata_probe VALUES(1)')
            assert run('SELECT v FROM public.metadata_probe')=='1\n'
            c.execute('UPDATE public.metadata_probe SET v=2')
            assert run('SELECT v FROM public.metadata_probe')=='2\n'
            results.append({'name':'query_results_reexecuted_after_external_commit','passed':True})
            commands=["SET work_mem='12345kB'; CREATE TEMP TABLE leftover(v int); SELECT pg_advisory_lock(8901547);",
                      "BEGIN; CREATE TABLE public.must_rollback(v int); INSERT INTO public.must_rollback VALUES(7);",
                      "DO $$ BEGIN PERFORM 1; END $$; SELECT 42 AS answer;"]
            for command in commands:assert run(command)==run(command,native=True)
            assert run("SELECT current_setting('work_mem')||'|'||(to_regclass('pg_temp.leftover') IS NULL)::text||'|'||(to_regclass('public.must_rollback') IS NULL)::text")==run("SELECT current_setting('work_mem')||'|'||(to_regclass('pg_temp.leftover') IS NULL)::text||'|'||(to_regclass('public.must_rollback') IS NULL)::text",native=True)
            assert c.execute('SELECT pg_try_advisory_lock(8901547)').fetchone()[0]
            c.execute('SELECT pg_advisory_unlock(8901547)')
            results.append({'name':'guc_temp_open_transaction_advisory_state_cleaned','passed':True})
            command='CREATE TABLE public.failed_batch(v int); INSERT INTO public.failed_batch VALUES(1); SELECT 1/0;'
            native=probe.server.ORIGINAL_RUN(base+['-qAt','-c',command],None,cwd=None,timeout=20)
            assert native.returncode and 'division by zero' in native.stderr
            try:run(command)
            except RuntimeError as error:assert 'division by zero' in str(error)
            else:raise AssertionError('Native error was lost')
            assert c.execute("SELECT to_regclass('public.failed_batch') IS NULL").fetchone()[0]
            assert run('SELECT 1')=='1\n'
            results.append({'name':'one_request_multi_statement_implicit_rollback_and_retry','passed':True})
    finally:probe.close_sessions(service)
    write_report('reports/native-metadata-session-isolation.json',{'database':db,'passed':True,'hook_sha256':probe.source_identity(),'results_cached':False,'cases':results})
    print('Native metadata isolation:',len(results),'cases PASS',flush=True)


if __name__=='__main__':main()
