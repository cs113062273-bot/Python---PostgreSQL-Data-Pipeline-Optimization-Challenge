"""Actual refined server-file COPY faults, complete rollback and native retry."""
import hashlib
import re
import threading
import time
import uuid
from bench.common import connect,write_report
from bench.fixture import create_fixture
from bench.control import seed_initial_versions
from bench.run import execute as baseline
from bench import refined_copy_probe as refined,fast_transport_probe as transport,server_copy_probe as server
from pipeline.local_service import LocalService
from pipeline.bulk_fks import BulkFKMixin
from pipeline.bulk_indexes import BulkIndexMixin
from pipeline.guarded_normalization import GuardedNormalizationMixin
from pipeline.native_copy import MARKER
from pipeline.recipes import ORDER,destinations,bulk_unique_tables,compiler_sha256
from pipeline.upstream import PostgresAdminService
from tests.equivalence.oracle import snapshot,assert_exact,protected_schema_signature,sequence_signature
from tests.recovery.native_copy_atomic import source_sha


def main(*,cases_override=None,marker_override=None,stage_relation='pgdm_pipeline_work.pipeline_normalized_input'):
    assert stage_relation in ('pgdm_pipeline_work.pipeline_normalized_input','pg_temp.pipeline_native_temp_input_v1')
    db='lab_refined_atomic_'+uuid.uuid4().hex[:8];create_fixture(db);seed_initial_versions(db)
    identity=refined.source_identity();refined.install();transport.install()
    native=server.ORIGINAL_RUN
    def injected(args,text,*,cwd,timeout=None,cpu=None):
        service=server.SERVICE.get()
        if service is not None and getattr(service,'fault',None) and isinstance(text,str) and MARKER in text:
            point,kind=service.fault
            if marker_override is not None and point in ('before_details','after_details','after_details_receipts','before_main','after_main','after_main_receipt'):
                marker=marker_override(point,text)
            elif point.startswith('copy_to_') or point.startswith('copy_from_'):
                direction='TO' if point.startswith('copy_to_') else 'FROM';table=point.split('_',2)[2]
                lines=[line for line in text.splitlines() if line.startswith('COPY ') and re.search(r"\b"+direction+r" '[^']*/"+table+r"\.bin' \(FORMAT BINARY\);$",line)]
                assert len(lines)==1,(point,lines);marker=lines[0]
            elif point=='after_projection':marker='UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=5;'
            elif point=='before_fk_restore':marker="INSERT INTO pg_temp.pgdm_expansion_timings VALUES\n      (8000,'bulk_load'"
            elif point=='after_fk_restore':marker='UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=8000;'
            elif point=='before_commit':marker="INSERT INTO pg_temp.pgdm_expansion_timings VALUES (9000, 'execute'"
            else:raise AssertionError(point)
            assert text.count(marker)==1
            if kind=='bad_frame':
                assert point=='before_details'
                replacement="UPDATE "+stage_relation+" SET pipeline_axes=decode('01','hex');\n"+marker
            elif kind=='missing_file':
                assert point.startswith('copy_from_');table=point.split('_',2)[2]
                replacement=marker.replace('/'+table+'.bin', '/missing.bin')
            elif kind=='bad_binary':
                assert point.startswith('copy_to_')
                target=re.search(r"TO ('[^']*') \(FORMAT BINARY\);$",marker)[1]
                replacement=marker+"\nCOPY (SELECT 'bad signature'::text) TO "+target+' (FORMAT TEXT);'
            elif kind=='identity_exhaustion':
                assert point=='before_main'
                replacement='ALTER SEQUENCE public.table1_id_column_1_seq RESTART WITH 1 MAXVALUE 2 CACHE 1;\n'+marker
            else:
                fault='SELECT pg_sleep(30);' if kind=='backend_termination' else "DO $$ BEGIN RAISE EXCEPTION 'refined_injected_"+point+"'; END $$;"
                replacement=(fault+'\n'+marker) if point.startswith('before_') else (marker+'\n'+fault)
            text="SET application_name='"+service.tag+"';\n"+text.replace(marker,replacement,1)
            service.injected_sha256=hashlib.sha256(text.encode()).hexdigest();service.applied=True
        return native(args,text,cwd=cwd,timeout=timeout,cpu=cpu)
    server.ORIGINAL_RUN=injected
    class Service(BulkFKMixin,BulkIndexMixin,GuardedNormalizationMixin,LocalService):
        bulk_foreign_keys=guarded_normalization=True
        bulk_recipe_mode='guarded_copy'
        bulk_unique_tables=bulk_unique_tables('guarded_copy')
        fault=None
    def run(service,register=False):
        timing=PostgresAdminService.create_cross_table_expansion_timing_report(db,'public.raw_data',['group001'],[d['table_name'] for d in destinations()])
        result=service.execute_cross_table_expansion(db,'public.raw_data',['group001'],destinations('guarded_copy'),timing_report=timing)
        if register:return result,service.register_cross_table_expansion_versions(result,'research','local-refined-fault-retry',timing_report=timing)
        transport.close_sessions(service)
        return result
    report={'kind':'refined_native_server_copy_atomic_faults','database':db,'compiler_sha256':compiler_sha256(),'refined_hook_sha256':identity,'tests':[],'all_passed':False,'scope':'64 generated rows, forced pre-commit failures and same-fixture retry. All final/source/counter/version data remain logged; only disposable normalized cache is unlogged. This is not a crash/recovery performance score.'}
    with connect(db,autocommit=True) as c:
        baseline(c,'baseline',['group001']);snapshot(c)
        c.execute('TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY')
        assert run(Service())['execution_architecture']['safe_domain']
        assert_exact(c)
        c.execute('TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY')
        c.execute("UPDATE public.pgdm_table_row_counts SET row_count=0 WHERE schema_name='public'")
        schema=protected_schema_signature(c);sequences=sequence_signature(c);source=source_sha(c)
        points=['after_projection','copy_to_table1','copy_from_table1','copy_from_table2','before_fk_restore','after_fk_restore','before_commit']
        cases=[(point,kind) for point in points for kind in ('server_exception','backend_termination')]+[('copy_from_table1','missing_file'),('copy_to_table1','bad_binary')]
        if cases_override is not None:cases=list(cases_override)
        for point,kind in cases:
            assert refined.source_identity()==identity
            service=Service();service.fault=(point,kind);service.tag='refined_fault_'+uuid.uuid4().hex;service.applied=False
            outcome={}
            def worker():
                try:run(service)
                except Exception as error:outcome['error']=str(error)
                finally:
                    state=service._expansion_timing_local
                    outcome['cleanup']=getattr(state,'copy_workspace_result',None)
                    outcome['metadata_sessions_remaining']=len(getattr(state,'metadata_sessions',{}))
            thread=threading.Thread(target=worker);thread.start();victim=None
            if kind=='backend_termination':
                deadline=time.monotonic()+30
                while thread.is_alive() and time.monotonic()<deadline:
                    victim=c.execute("SELECT pid,extract(epoch FROM backend_start)::text FROM pg_stat_activity WHERE datname=%s AND application_name=%s AND backend_type='client backend' AND state='active' AND query LIKE 'SELECT pg_sleep%%'",(db,service.tag)).fetchone()
                    if victim:break
                    time.sleep(.05)
                assert victim is not None,(point,outcome)
                terminated=c.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=%s AND extract(epoch FROM backend_start)=%s::numeric AND datname=%s AND application_name=%s AND query LIKE 'SELECT pg_sleep%%'",(*victim,db,service.tag)).fetchone()
                assert terminated==(True,)
            thread.join(45)
            assert not thread.is_alive() and service.applied and outcome.get('error'),(point,kind,outcome)
            if kind=='server_exception':assert 'refined_injected_'+point in outcome['error'],outcome
            if kind=='missing_file':assert 'missing.bin' in outcome['error'],outcome
            if kind=='bad_binary':assert 'signature' in outcome['error'],outcome
            if kind=='bad_frame':assert 'frame length is invalid' in outcome['error'],outcome
            if kind=='identity_exhaustion':assert 'maximum value' in outcome['error'],outcome
            assert outcome['cleanup'] and outcome['cleanup']['cleanup_success'],outcome
            assert outcome['metadata_sessions_remaining']==0,outcome
            assert source_sha(c)==source and protected_schema_signature(c)==schema and sequence_signature(c)==sequences
            assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work') IS NULL").fetchone()[0]
            assert all(c.execute('SELECT count(*) FROM public.'+table).fetchone()[0]==0 for table in ORDER)
            assert c.execute("SELECT sum(row_count) FROM public.pgdm_table_row_counts WHERE schema_name='public'").fetchone()[0]==0
            report['tests'].append({'boundary':point,'failure':kind,'passed':True,'actual_injected_batch_sha256':service.injected_sha256,'source_schema_sequence_counters_rolled_back':True,'files_and_metadata_sessions_cleaned':True,'victim_identity':victim})
            write_report('reports/recovery-refined-copy-atomic.json',report);print(point,kind,'PASS',flush=True)
        retry_service=Service();result,versions=run(retry_service,register=True)
        assert_exact(c);assert len(versions)==6
        assert source_sha(c)==source and protected_schema_signature(c)==schema and sequence_signature(c)==sequences
        assert all(c.execute('SELECT count(*) FROM public.'+table).fetchone()[0]==c.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s",(table,)).fetchone()[0] for table in ORDER)
        assert c.execute("SELECT bool_and(relpersistence='p') FROM pg_class WHERE oid=ANY(%s::regclass[])",(['public.raw_data','public.pgdm_table_row_counts']+['public.'+t for t in ORDER],)).fetchone()[0]
        report.update({'all_passed':len(report['tests'])==len(cases),'final_native_retry_exact_outputs':True,'retry_counters':True,'retry_six_versions':len(versions),'original_source_sha256':source,'retry_execution_architecture':result['execution_architecture'],'retry_local_backend_diagnostics':getattr(retry_service._expansion_timing_local,'local_backend_diagnostics',None)})
        write_report('reports/recovery-refined-copy-atomic.json',report)
        print('Refined native atomic:',len(cases),'faults and exact retry/six versions PASS',flush=True)


if __name__=='__main__':main()
