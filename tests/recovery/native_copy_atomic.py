"""Actual native COPY rollback at server, client-file and backend boundaries."""
import hashlib
import threading
import time
import uuid
from bench.common import connect,write_report
from bench.control import seed_initial_versions
from bench.fixture import create_fixture
from pipeline.local_service import LocalService
from pipeline.bulk_fks import BulkFKMixin
from pipeline.bulk_indexes import BulkIndexMixin
from pipeline.guarded_normalization import GuardedNormalizationMixin
from pipeline.recipes import ORDER,destinations,bulk_unique_tables
from pipeline.upstream import PostgresAdminService
from tests.equivalence.oracle import protected_schema_signature,sequence_signature


def source_sha(c):
    digest=hashlib.sha256()
    with c.cursor().copy('COPY (SELECT * FROM public.raw_data ORDER BY raw_id) TO STDOUT (FORMAT BINARY)') as stream:
        for block in stream:digest.update(block)
    return digest.hexdigest()


def main():
    db='lab_native_atomic_'+uuid.uuid4().hex[:8]
    create_fixture(db);seed_initial_versions(db)
    class FaultLocal(LocalService):
        fault=None
        def run_remote_command(self,command,stdin_text=None,**kwargs):
            if self.fault and isinstance(stdin_text,str) and 'BEGIN ISOLATION LEVEL REPEATABLE READ;' in stdin_text and 'pgdm_cross_table_hash_manifest' in stdin_text:
                point,kind=self.fault
                self.applied=True
                if point.startswith('copy_to_'):
                    table=point.removeprefix('copy_to_')
                    markers=[line for line in stdin_text.splitlines() if line.startswith('\\copy (') and "TO '"+table+".bin'" in line]
                    assert len(markers)==1
                    marker=markers[0]
                elif point.startswith('copy_from_'):
                    table=point.removeprefix('copy_from_')
                    markers=[line for line in stdin_text.splitlines() if line.startswith('\\copy public.'+table+' (')]
                    assert len(markers)==1
                    marker=markers[0]
                elif point=='before_commit':marker="INSERT INTO pg_temp.pgdm_expansion_timings VALUES (9000, 'execute'"
                elif point=='before_fk_restore':marker="INSERT INTO pg_temp.pgdm_expansion_timings VALUES\n      (8000,'bulk_load'"
                else:
                    sequence={'after_projection':5,'after_fk_drop':4,'after_table1_prepare':110,'after_table1_restore':111,'after_fk_restore':8000}[point]
                    marker='UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number='+str(sequence)+';'
                assert stdin_text.count(marker)==1,(point,marker)
                if kind=='missing_file':
                    assert point=='copy_from_table1'
                    stdin_text=stdin_text.replace(marker,marker.replace("FROM 'table1.bin'","FROM 'missing.bin'"),1)
                elif kind=='bad_binary':
                    assert point=='copy_to_table1'
                    stdin_text=stdin_text.replace(marker,marker+"\n\\copy (SELECT 'bad signature'::text) TO 'table1.bin' (FORMAT TEXT)\n",1)
                else:
                    injected="SELECT pg_sleep(30);" if kind=='backend_termination' else "DO $native_fault$ BEGIN RAISE EXCEPTION 'pipeline_injected_"+point+"'; END $native_fault$;"
                    before=point.startswith('before_')
                    stdin_text=stdin_text.replace(marker,(injected+'\n'+marker) if before else (marker+'\n'+injected),1)
                stdin_text="SET application_name='"+self.tag+"';\n"+stdin_text
                self.injected_sha256=hashlib.sha256(stdin_text.encode()).hexdigest()
            return super().run_remote_command(command,stdin_text=stdin_text,**kwargs)
    class Service(BulkFKMixin,BulkIndexMixin,GuardedNormalizationMixin,FaultLocal):
        bulk_foreign_keys=guarded_normalization=True
        bulk_recipe_mode='guarded_copy'
        bulk_unique_tables=bulk_unique_tables('guarded_copy')
    with connect(db,autocommit=True) as observer:
        def execute(service):
            timing=PostgresAdminService.create_cross_table_expansion_timing_report(db,'public.raw_data',['group001'],[d['table_name'] for d in destinations()])
            return service.execute_cross_table_expansion(db,'public.raw_data',['group001'],destinations('guarded_copy'),timing_report=timing)
        initial=Service()
        assert execute(initial)['execution_architecture']['safe_domain'] is True
        observer.execute('TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY')
        observer.execute("UPDATE public.pgdm_table_row_counts SET row_count=0 WHERE schema_name='public'")
        before=protected_schema_signature(observer);sequences=sequence_signature(observer);sha=source_sha(observer)
        points=['after_projection','after_fk_drop','after_table1_prepare','copy_to_table1','copy_from_table1','after_table1_restore','copy_from_table2','before_fk_restore','after_fk_restore','before_commit']
        tests=[]
        cases=[(point,kind) for point in points for kind in ('server_exception','backend_termination')]+[('copy_from_table1','missing_file'),('copy_to_table1','bad_binary')]
        for point,kind in cases:
            service=Service();service.fault=(point,kind);service.applied=False;service.tag='copy_fault_'+uuid.uuid4().hex
            outcome={}
            def worker():
                try:execute(service)
                except Exception as error:outcome['error']=str(error)
                finally:outcome['cleanup']=getattr(service._expansion_timing_local,'copy_workspace_result',None)
            thread=threading.Thread(target=worker)
            thread.start()
            if kind=='backend_termination':
                deadline=time.monotonic()+30
                victim=None
                while thread.is_alive() and time.monotonic()<deadline:
                    row=observer.execute("SELECT pid FROM pg_stat_activity WHERE datname=%s AND application_name=%s AND state='active' AND query LIKE 'SELECT pg_sleep%%'",(db,service.tag)).fetchone()
                    if row:victim=row[0];break
                    time.sleep(0.05)
                assert victim is not None,(point,outcome)
                assert observer.execute('SELECT pg_terminate_backend(%s)',(victim,)).fetchone()[0]
            thread.join(45)
            assert not thread.is_alive() and service.applied and 'error' in outcome,(point,kind,outcome)
            if kind=='server_exception':assert 'pipeline_injected_'+point in outcome['error'],outcome
            if kind=='missing_file':assert 'missing.bin' in outcome['error'],outcome
            if kind=='bad_binary':assert 'signature' in outcome['error'],outcome
            assert outcome['cleanup'] and outcome['cleanup']['cleanup_success'],outcome
            assert protected_schema_signature(observer)==before,(point,'schema')
            assert sequence_signature(observer)==sequences,(point,'sequences')
            assert source_sha(observer)==sha,(point,'source')
            assert observer.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()[0] is None
            assert all(observer.execute('SELECT count(*) FROM public.'+table).fetchone()[0]==0 for table in ORDER)
            assert observer.execute("SELECT sum(row_count) FROM public.pgdm_table_row_counts WHERE schema_name='public'").fetchone()[0]==0
            tests.append({'boundary':point,'failure':kind,'batch_sha256':service.injected_sha256,'passed':True,'copy_cleanup_success':True})
            print(point,kind,': PASS',flush=True)
            write_report('reports/recovery-native-copy-atomic.json',{'database':db,'tests':tests,'source_sha256':sha,'all_passed':len(tests)==len(cases),'scope':'Actual native service, COPY command receipts/counters, protected schema and sequence options, source SHA and workspace/file cleanup; forced pre-commit failures; ordinary identity gaps allowed.'})


if __name__=='__main__':main()
