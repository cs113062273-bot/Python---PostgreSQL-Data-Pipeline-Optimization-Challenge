"""Changed receiving layouts must use original assignment casts, not binary."""
import os
from pathlib import Path
import re
import subprocess
import tempfile
import uuid
from bench.common import connect,ROOT,write_report
from bench.fixture import create_fixture
from bench.control import seed_initial_versions
from pipeline.local_service import LocalService
from pipeline.bulk_fks import BulkFKMixin
from pipeline.bulk_indexes import BulkIndexMixin
from pipeline.guarded_normalization import GuardedNormalizationMixin
from pipeline.recipes import ORDER,destinations,bulk_unique_tables
from pipeline.upstream import PostgresAdminService
from tests.equivalence.oracle import canonical,protected_schema_signature,sequence_signature


def main(*,extra_variants=(),output='reports/semantics-copy-layout.json'):
    db='lab_copy_layout_'+uuid.uuid4().hex[:8]
    create_fixture(db);seed_initial_versions(db)
    scripts={}
    class Capture(LocalService):
        def run_remote_command(self,command,stdin_text=None,**kwargs):
            if isinstance(stdin_text,str) and 'BEGIN ISOLATION LEVEL REPEATABLE READ;' in stdin_text and 'pgdm_cross_table_hash_manifest' in stdin_text:scripts[self.bulk_recipe_mode]=stdin_text
            return super().run_remote_command(command,stdin_text=stdin_text,**kwargs)
    class Service(BulkFKMixin,BulkIndexMixin,GuardedNormalizationMixin,Capture):pass
    def reset(c):
        c.execute('TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY')
        c.execute("UPDATE public.pgdm_table_row_counts SET row_count=0 WHERE schema_name='public'")
    variants=[
      ('timestamp_receiving_type','ALTER TABLE public.table1 ALTER COLUMN datetime_column_1 TYPE timestamptz','ALTER TABLE public.table1 ALTER COLUMN datetime_column_1 TYPE timestamp'),
      ('integer_receiving_width','ALTER TABLE public.table1 ALTER COLUMN integer_column_3 TYPE smallint','ALTER TABLE public.table1 ALTER COLUMN integer_column_3 TYPE integer'),
      ('numeric_typmod','ALTER TABLE public.table5 ALTER COLUMN numeric_column_1 TYPE numeric(3,1)','ALTER TABLE public.table5 ALTER COLUMN numeric_column_1 TYPE numeric'),
      ('varchar_typmod','ALTER TABLE public.table3 ALTER COLUMN text_column_1 TYPE varchar(1)','ALTER TABLE public.table3 ALTER COLUMN text_column_1 TYPE varchar'),
      ('extra_unique_index','CREATE UNIQUE INDEX user_layout_unique ON public.table5(numeric_column_1)','DROP INDEX public.user_layout_unique'),
      ('extra_check','ALTER TABLE public.table1 ADD CONSTRAINT user_layout_check CHECK(integer_column_6 IS NULL)','ALTER TABLE public.table1 DROP CONSTRAINT user_layout_check'),
    ]
    variants+=list(extra_variants)
    results=[]
    with connect(db,autocommit=True) as c:
        for mode in ('baseline','guarded_copy'):
            service=Service();service.bulk_recipe_mode=mode
            service.bulk_foreign_keys=service.guarded_normalization=mode!='baseline'
            service.bulk_unique_tables=bulk_unique_tables(mode)
            timing=PostgresAdminService.create_cross_table_expansion_timing_report(db,'public.raw_data',['group001'],[d['table_name'] for d in destinations()])
            result=service.execute_cross_table_expansion(db,'public.raw_data',['group001'],destinations(mode),timing_report=timing)
            if mode=='guarded_copy':assert result['execution_architecture']['safe_domain'] is True
            reset(c)
        # Observe the actual layout-domain choice, independently of other
        # legitimate protocol receipts that may also use psql \gset.
        selection=r'(?m)(^SELECT safe AS pipeline_copy_safe[^\n]*\n)\\gset\n'
        assert len(re.findall(selection,scripts['guarded_copy']))==1
        scripts['guarded_copy']=re.sub(selection,lambda m:m[0]+'\\echo LAYOUT_FAST=:pipeline_copy_safe\n',scripts['guarded_copy'],count=1)
        parent=ROOT/'.runtime'/'copy_layout';parent.mkdir(parents=True,exist_ok=True)
        original=protected_schema_signature(c)
        for variant in variants:
            name,setup,restore=variant[:3]
            expected_fast=variant[3] if len(variant)>3 else False
            reset(c);c.execute(setup)
            before=protected_schema_signature(c);sequence_before=sequence_signature(c)
            outcomes={}
            for mode in ('baseline','guarded_copy'):
                reset(c)
                directory=Path(tempfile.mkdtemp(prefix='trial_',dir=parent)).resolve()
                assert directory.parent==parent.resolve()
                try:
                    result=subprocess.run([os.environ.get('CHALLENGE_PSQL','psql'),'-X','-q','-A','-t','-w','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-d',db],input=scripts[mode],text=True,encoding='utf-8',errors='replace',capture_output=True,cwd=directory)
                finally:
                    for t in ORDER:(directory/(t+'.bin')).unlink(missing_ok=True)
                    directory.rmdir()
                if mode=='guarded_copy':
                    assert 'LAYOUT_FAST=' in result.stdout
                    fast_chosen='LAYOUT_FAST=t' in result.stdout
                    if expected_fast is not None:assert fast_chosen is expected_fast,(name,result.stdout,result.stderr)
                assert protected_schema_signature(c)==before and sequence_signature(c)==sequence_before
                if result.returncode:
                    m=re.search(r'ERROR:\s+([0-9A-Z]{5}):\s*(.*?)(?=\n(?:DETAIL:|HINT:|CONTEXT:|LOCATION:)|\Z)',result.stderr,re.S)
                    assert m,result.stderr
                    outcomes[mode]={'sqlstate':m[1],'primary':m[2].strip()}
                else:outcomes[mode]={'sqlstate':'00000','outputs':{t:[r[0] for r in c.execute('SELECT value::text FROM ('+canonical(t)+') t ORDER BY value::text')] for t in ORDER}}
                for table in ORDER:
                    actual=c.execute('SELECT count(*) FROM public.'+table).fetchone()[0]
                    counter=c.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s",(table,)).fetchone()[0]
                    assert actual==counter
                    if result.returncode:assert actual==0
                assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()[0] is None
            assert outcomes['baseline']==outcomes['guarded_copy'],(name,outcomes)
            reset(c);c.execute(restore)
            assert protected_schema_signature(c)==original
            results.append({'name':name,'sqlstate':outcomes['baseline']['sqlstate'],'exact_original_fallback':not fast_chosen,'fast_path_chosen':fast_chosen,'passed':True})
            print(name,': PASS',flush=True)
    write_report(output,{'database':db,'cases':results,'all_passed':True,'scope':'Native batch engine with intentionally altered receiving metadata; specified rejected layouts must use the original path. A benign DEFAULT NULL may be optimized away by PostgreSQL. All original assignment semantics/counters/schema retained.'})


if __name__=='__main__':main()
