"""Native service fast/fallback/workspace-collision branches and six versions."""
import json
import subprocess
import sys
import uuid
from bench.common import connect,ROOT,write_report
from bench.fixture import create_fixture


def main():
    cases=[('ctas_fast','guarded_ctas',True),('copy_valid_fallback','guarded_copy',False),('copy_namespace_collision','guarded_copy',False)]
    results=[]
    for name,mode,fast in cases:
        db='lab_native_branch_'+uuid.uuid4().hex[:8]
        create_fixture(db)
        with connect(db,autocommit=True) as c:
            if name=='copy_valid_fallback':c.execute("UPDATE public.raw_data SET raw=jsonb_set(raw,'{values,numeric_column_1}','\"1e2\"'::jsonb) WHERE raw_id=(SELECT min(raw_id) FROM public.raw_data)")
            if name=='copy_namespace_collision':c.execute("CREATE SCHEMA pgdm_pipeline_work; CREATE TABLE pgdm_pipeline_work.user_sentinel(v text); INSERT INTO pgdm_pipeline_work.user_sentinel VALUES('preserve')")
        path=ROOT/'reports'/('local-service-'+name+'64.json')
        subprocess.run([sys.executable,'-m','bench.service_run','--database',db,'--mode',mode,'--output',str(path)],check=True,cwd=ROOT)
        value=json.loads(path.read_text())
        assert value['execution_architecture']['safe_domain'] is fast
        assert value['protected_schema_equivalent'] and value['sequence_options_and_ownership_equivalent'] and value['workspace_namespace_restored']
        assert value['exact_six_tables'] and value['counter_reconciliation'] and len(value['versions'])==6
        if mode=='guarded_copy':assert value['execution_architecture']['copy_workspace']['cleanup_success']
        if name=='copy_namespace_collision':
            with connect(db,autocommit=True) as c:assert c.execute('SELECT v FROM pgdm_pipeline_work.user_sentinel').fetchall()==[('preserve',)]
        results.append({'name':name,'database':db,'mode':mode,'fast':fast,'all_passed':True,'report':str(path.relative_to(ROOT))})
    write_report('reports/recovery-native-branches.json',{'kind':'native_fast_valid_fallback_and_unrelated_workspace_preservation','tests':results,'all_passed':True})


if __name__=='__main__':main()
