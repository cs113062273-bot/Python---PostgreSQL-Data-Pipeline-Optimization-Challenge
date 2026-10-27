"""Preserve legacy source SHA and resume existing jobs after streaming changes."""
import argparse
from contextlib import contextmanager
import json
import threading
import time
import uuid
from unittest.mock import patch

import psycopg

from bench.common import ROOT,connect,require_lab,write_report
from bench.fixture import create_fixture
from pipeline import checkpoints
from tests.recovery.run import Service,fixture,check


SETTINGS=['max_parallel_workers_per_gather','min_parallel_table_scan_size','parallel_setup_cost','parallel_tuple_cost','work_mem']


def settings(c):
    return c.execute('SELECT name,current_setting(name) FROM unnest(%s::text[]) AS s(name)',(SETTINGS,)).fetchall()


@contextmanager
def rollback(c):
    c.execute('BEGIN')
    try:yield
    finally:c.execute('ROLLBACK')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--large-database',help='Optional existing disposable 1M source for real parallel/GUC/cancel checks')
    p.add_argument('--legacy-report',default='reports/source-hash-probe-1m.json')
    p.add_argument('--output',default='reports/recovery-source-fingerprint.json')
    args=p.parse_args()
    db='lab_source_sha_'+uuid.uuid4().hex[:8];create_fixture(db)
    results=[]
    mutations=[
        ('unchanged',None),
        ('json_edit_same_stored_hash',"UPDATE public.raw_data SET raw=jsonb_set(raw,'{values,text_column_1}',to_jsonb('edited'::text)) WHERE raw_id=1"),
        ('raw_sql_null','UPDATE public.raw_data SET raw=NULL WHERE raw_id=1'),
        ('raw_json_null',"UPDATE public.raw_data SET raw='null'::jsonb WHERE raw_id=1"),
        ('unicode_and_controls',"UPDATE public.raw_data SET raw=jsonb_build_object('unicode',chr(20013)||chr(25991)||chr(10)||chr(9)||chr(92)) WHERE raw_id=1"),
        ('stored_hash_null','UPDATE public.raw_data SET raw_hash=NULL WHERE raw_id=1'),
        ('stored_hash_changed',"UPDATE public.raw_data SET raw_hash='different' WHERE raw_id=1"),
        ('ingestion_null','UPDATE public.raw_data SET raw_ingested_at=NULL WHERE raw_id=1'),
        ('ingestion_changed',"UPDATE public.raw_data SET raw_ingested_at=raw_ingested_at+interval '1 microsecond' WHERE raw_id=1"),
        ('schema_leaves_scope',"UPDATE public.raw_data SET raw_schema='other' WHERE raw_id=1"),
        ('schema_null','UPDATE public.raw_data SET raw_schema=NULL WHERE raw_id=1'),
        ('tab_controls',"UPDATE public.raw_data SET raw_tab=chr(9)||chr(10)||chr(92) WHERE raw_id=1"),
        ('tab_null','UPDATE public.raw_data SET raw_tab=NULL WHERE raw_id=1'),
        ('negative_id','UPDATE public.raw_data SET raw_id=-9223372036854775808 WHERE raw_id=1'),
        ('maximum_id','UPDATE public.raw_data SET raw_id=9223372036854775807 WHERE raw_id=1'),
    ]
    # Deliberate raw_id type changes invalidate a prepared SELECT result type;
    # keep these isolated DDL variants out of psycopg's prepared-plan cache.
    with connect(db,autocommit=True,prepare_threshold=None) as c:
        for zone in ('Asia/Taipei','UTC','America/New_York'):
            c.execute("SELECT set_config('TimeZone',%s,false)",(zone,))
            baseline=checkpoints.source_digest_legacy(c,['group001'])
            for name,statement in mutations:
                with rollback(c):
                    stored=c.execute('SELECT raw_hash FROM public.raw_data WHERE raw_id=1').fetchone()[0]
                    if statement:c.execute(statement)
                    before=settings(c)
                    expected=checkpoints.source_digest_legacy(c,['group001'])
                    actual=checkpoints.source_digest(c,['group001'])
                    assert actual==expected and settings(c)==before,(zone,name)
                    if statement:assert actual!=baseline,(zone,name)
                    if name=='json_edit_same_stored_hash':assert c.execute('SELECT raw_hash FROM public.raw_data WHERE raw_id=1').fetchone()[0]==stored
                results.append({'name':name,'zone':zone,'legacy_digest_identical':True})
        for scopes in ([],['missing'],['group001','group001','missing']):
            assert checkpoints.source_digest(c,scopes)==checkpoints.source_digest_legacy(c,scopes)
            results.append({'name':'scope_shape','scopes':scopes,'legacy_digest_identical':True})
        for name,statement in [
            ('extra_column',"ALTER TABLE public.raw_data ADD COLUMN extra text DEFAULT 'bind this too'"),
            ('altered_id_type','ALTER TABLE public.raw_data ALTER COLUMN raw_id TYPE integer'),
            ('row_security','ALTER TABLE public.raw_data ENABLE ROW LEVEL SECURITY'),
            ('inherited_source','CREATE TABLE challenge_lab.source_child () INHERITS (public.raw_data)'),
            ('missing_primary_key','ALTER TABLE public.table1 DROP CONSTRAINT table1_id_column_3_fkey; ALTER TABLE public.raw_data DROP CONSTRAINT raw_data_pkey')]:
            with rollback(c):
                c.execute(statement)
                expected=checkpoints.source_digest_legacy(c,['group001'])
                with patch.object(checkpoints,'_source_digest_copy',side_effect=AssertionError('Noncanonical source used parallel helper')):
                    assert checkpoints.source_digest(c,['group001'])==expected
            results.append({'name':name,'exact_legacy_fallback':True})
    # Persist a real committed chunk using the old SHA implementation, then
    # resume the exact same request/receipt through the new implementation.
    resume_db=fixture();job=str(uuid.uuid4());service=Service()
    class Interrupted(RuntimeError):pass
    def interrupt(event,details):
        if event=='after_chunk_commit':raise Interrupted('legacy checkpoint persisted')
    with patch.object(checkpoints,'source_digest',checkpoints.source_digest_legacy):
        try:checkpoints.ChunkRunner(service,connect,interrupt).run(resume_db,['group001'],job,16)
        except Interrupted:pass
        else:raise AssertionError('Legacy checkpoint boundary not reached')
    with connect(resume_db,autocommit=True) as c:
        old_sha=c.execute('SELECT source_sha256 FROM pipeline_recovery.jobs WHERE job_id=%s',(job,)).fetchone()[0]
        first=c.execute('SELECT lo,hi,completed_at FROM pipeline_recovery.chunks WHERE job_id=%s ORDER BY lo',(job,)).fetchall()
        assert len(first)==1
    runner=checkpoints.ChunkRunner(service,connect)
    runner.run(resume_db,['group001'],job,16)
    versions=service.finalize_checkpoint_job(runner,resume_db,job)
    assert len(versions)==6
    chunks=check(resume_db,job)
    assert chunks[:1]==first
    with connect(resume_db,autocommit=True) as c:assert c.execute('SELECT source_sha256 FROM pipeline_recovery.jobs WHERE job_id=%s',(job,)).fetchone()[0]==old_sha
    results.append({'name':'actual_legacy_job_resume','database':resume_db,'job':job,'committed_prefix_timestamp_preserved':True,'source_sha_not_rewritten':True,'exact_six_tables_and_counters':True,'versions':6})
    if args.large_database:
        reference=json.loads((ROOT/args.legacy_report).read_text())
        assert reference['database']==args.large_database and reference['json_variants_same_legacy_digest']
        expected=next(r['digest'] for r in reference['runs'] if r['mode']=='json_batched')
        with connect(args.large_database,autocommit=True) as c:
            require_lab(c)
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY raw_schema')]
            c.execute("SELECT set_config('TimeZone',%s,false)",(reference['environment']['settings']['TimeZone'],))
            c.execute("SET max_parallel_workers_per_gather=1; SET min_parallel_table_scan_size='16MB'; SET parallel_setup_cost=435; SET parallel_tuple_cost=.13; SET work_mem='768kB'")
            before=settings(c);started=time.perf_counter()
            assert checkpoints.source_digest(c,scopes)==expected
            seconds=time.perf_counter()-started
            assert settings(c)==before
            results.append({'name':'actual_large_parallel_path_and_settings_restore','database':args.large_database,'seconds':seconds,'legacy_digest_identical':True,'caller_settings_restored':True})
            # Actual chunk initialization adds a source write-fence trigger
            # before hashing. Exercise that DDL/lock context at the large scale;
            # roll back every temporary catalog object on the retained template.
            name='source_sha_probe_'+uuid.uuid4().hex[:10]
            with c.transaction(force_rollback=True):
                c.execute('LOCK TABLE public.raw_data IN SHARE ROW EXCLUSIVE MODE')
                c.execute('CREATE FUNCTION challenge_lab.'+name+'() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$')
                c.execute('CREATE TRIGGER '+name+' BEFORE INSERT ON public.raw_data FOR EACH STATEMENT EXECUTE FUNCTION challenge_lab.'+name+'()')
                started=time.perf_counter()
                assert checkpoints.source_digest(c,scopes)==expected
                ddl_seconds=time.perf_counter()-started
                assert settings(c)==before
            assert c.execute('SELECT to_regprocedure(%s)',('challenge_lab.'+name+'()',)).fetchone()[0] is None
            results.append({'name':'large_source_hash_after_transactional_trigger_ddl','seconds':ddl_seconds,'legacy_digest_identical':True,'probe_ddl_rolled_back':True,'caller_settings_restored':True})
            cancelled=[];observer_errors=[]
            def cancel():
                try:
                    with connect('postgres',autocommit=True) as observer:
                        deadline=time.monotonic()+15
                        while time.monotonic()<deadline:
                            row=observer.execute("SELECT query FROM pg_stat_activity WHERE pid=%s AND state='active'",(c.info.backend_pid,)).fetchone()
                            if row and row[0].startswith('COPY (SELECT raw_id::text'):
                                cancelled.append(observer.execute('SELECT pg_cancel_backend(%s)',(c.info.backend_pid,)).fetchone()[0]);return
                            time.sleep(.01)
                        raise RuntimeError('Source COPY query not observed')
                except Exception as error:observer_errors.append(str(error))
            thread=threading.Thread(target=cancel,daemon=True);thread.start()
            try:checkpoints.source_digest(c,scopes)
            except psycopg.errors.QueryCanceled:pass
            else:raise AssertionError('Native source COPY cancellation did not interrupt')
            thread.join(20)
            assert not thread.is_alive() and cancelled==[True] and not observer_errors,observer_errors
            assert settings(c)==before and c.execute('SELECT 1').fetchone()[0]==1
            results.append({'name':'actual_copy_cancellation_restores_settings_and_connection','database':args.large_database,'passed':True})
    write_report(args.output,{'kind':'legacy_source_sha_compatibility_and_actual_checkpoint_resume','database':db,'passed':True,'cases':results,'case_count':len(results),'scope':'Actual PostgreSQL, untouched generator fixtures. Different TimeZone settings retain existing digest semantics within each session; this change does not redefine the old timezone-dependent hash contract. Large parallel path/cancel covered only when explicitly supplied.'})
    print('Source SHA compatibility / real old-job resume:',len(results),'cases PASS',flush=True)


if __name__=='__main__':main()
