"""Actual native stdin/c modes, formatting, lexical fallback and transaction boundaries."""
import os
import types
import uuid
from bench.common import connect,write_report
from bench.fixture import create_fixture
from bench import script_transport_probe as script,fast_transport_probe as fast
from pipeline.local_service import LocalService
from tests.recovery import metadata_sessions as ctests


def main():
    ctests.probe=types.SimpleNamespace(install=script.install,server=fast.server,eligible=fast.eligible,close_sessions=fast.close_sessions,source_identity=script.source_identity)
    ctests.write_report=lambda ignored,value:write_report('reports/native-script-c-session-isolation.json',value)
    ctests.main()
    db='lab_stdin_metadata_'+uuid.uuid4().hex[:8];create_fixture(db);service=LocalService()
    import pipeline.local_diagnostics as diagnostic
    base=[service.psql,'-h',os.environ['PGHOST'],'-p',os.environ['PGPORT'],'-U',os.environ['PGUSER'],'-d',db,'-X','-v','ON_ERROR_STOP=1']
    tests=[]
    def run(text,flags=('-qAt',),native=False):
        args=base+list(flags)+['-f','-']
        if native:return fast.server.ORIGINAL_RUN(args,text,cwd=None,timeout=15)
        token=fast.server.SERVICE.set(service)
        try:return diagnostic.run_psql(args,text,cwd=None,timeout=15)
        finally:fast.server.SERVICE.reset(token)
    try:
        for flags in (('-qAt',),('--csv',),(),('-t',)):
            text="DO $body$ BEGIN PERFORM ';'; END $body$; SELECT '中文' AS value,chr(10) AS newline; -- final comment\n"
            actual=run(text,flags);expected=run(text,flags,native=True)
            assert actual.returncode==expected.returncode==0 and actual.stdout==expected.stdout,(flags,actual.stdout,expected.stdout)
            tests.append({'name':'stdin_native_output_and_unicode_dollar_quotes','flags':flags,'passed':True})
        with connect(db,autocommit=True) as c:
            for explicit in (False,True):
                for native in (False,True):
                    table='script_'+('native' if native else 'reused')+('_tx' if explicit else '_auto')
                    text=('BEGIN;' if explicit else '')+'CREATE TABLE public.'+table+'(v int); INSERT INTO public.'+table+' VALUES(1); SELECT 1/0;'
                    if native:
                        result=run(text,native=True);assert result.returncode and 'division by zero' in result.stderr
                    else:
                        try:run(text)
                        except RuntimeError as error:assert 'division by zero' in str(error)
                        else:raise AssertionError('Stdin SQL error was lost')
                    present=c.execute('SELECT to_regclass(%s)',('public.'+table,)).fetchone()[0]
                    assert bool(present) is (not explicit)
                    if present:assert c.execute('SELECT count(*) FROM public.'+table).fetchone()[0]==1
                tests.append({'name':'explicit_transaction_rollback' if explicit else 'native_stdin_prior_statement_commits_preserved','passed':True})
            assert run('SELECT 1;').stdout=='1\n'
            run("CREATE TEMP TABLE leftover(v int); SET work_mem='12345kB'; SELECT pg_advisory_lock(8901548);")
            assert run("SELECT (to_regclass('pg_temp.leftover') IS NULL)::text||'|'||current_setting('work_mem');").stdout==run("SELECT (to_regclass('pg_temp.leftover') IS NULL)::text||'|'||current_setting('work_mem');",native=True).stdout
            assert c.execute('SELECT pg_try_advisory_lock(8901548)').fetchone()[0];c.execute('SELECT pg_advisory_unlock(8901548)')
            tests.append({'name':'stdin_temp_guc_advisory_state_reset','passed':True})
        for text in ("SELECT 'unterminated;",'DO $x$ BEGIN PERFORM 1;',"SELECT 1; /* open",'SELECT 1',"SELECT E'back\\slash';"):
            assert not script.complete_program(text)
        for text in ('SELECT 1; -- closed by EOF','/* a /* nested */ b */ SELECT 1;','DO $$ BEGIN PERFORM 1; END $$;'):
            assert script.complete_program(text)
        result=run("SELECT 'unterminated;")
        assert result.returncode and 'unterminated' in result.stderr
        tests.append({'name':'incomplete_escape_and_metacommand_fallback_without_pipe_hang','passed':True})
    finally:fast.close_sessions(service)
    write_report('reports/native-metadata-stdin-isolation.json',{'passed':True,'database':db,'hook_sha256':script.source_identity(),'cases':tests,'native_c_tests':11,'results_cached':False})
    print('Native stdin metadata:',len(tests),'cases plus 11 native -c tests PASS',flush=True)


if __name__=='__main__':main()
