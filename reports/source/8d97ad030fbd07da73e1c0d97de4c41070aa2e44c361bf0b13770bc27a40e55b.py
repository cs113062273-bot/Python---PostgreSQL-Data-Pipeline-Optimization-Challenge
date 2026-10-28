"""Reject altered internal loader metadata before executing C writes."""
import uuid
from bench import native_c_lab as lab,native_details_probe as candidate
from bench.common import write_report


def main():
    db='lab_details_guards_'+uuid.uuid4().hex[:8];lab.fixture(db)
    ext=candidate.package()['extension'];frozen=candidate.source_identity();checks=[]
    signature='pgdm_pipeline_work.details_load(regclass,regclass,regclass,regclass,regclass,regclass)'
    mutations=[('volatility','ALTER FUNCTION '+signature+' IMMUTABLE'),
               ('parallel','ALTER FUNCTION '+signature+' PARALLEL SAFE'),
               ('security','ALTER FUNCTION '+signature+' SECURITY DEFINER'),
               ('strict','ALTER FUNCTION '+signature+' CALLED ON NULL INPUT'),
               ('configuration','ALTER FUNCTION '+signature+' SET search_path TO public'),
               ('member_removed','ALTER EXTENSION '+ext+' DROP FUNCTION '+signature),
               ('renamed','ALTER FUNCTION '+signature+' RENAME TO changed_details'),
               ('identity_volatility','ALTER FUNCTION pgdm_pipeline_work.details_identity() VOLATILE')]
    with lab.connect(db,autocommit=True) as c:
        for name,change in mutations:
            with c.transaction(force_rollback=True):
                c.execute('CREATE SCHEMA pgdm_pipeline_work; CREATE EXTENSION '+ext+' WITH SCHEMA pgdm_pipeline_work')
                assert c.execute('SELECT '+candidate.member_guard()).fetchone()==(True,)
                c.execute(change)
                assert c.execute('SELECT '+candidate.member_guard()).fetchone()==(False,),name
            assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()==(None,)
            checks.append({'mutation':name,'rejected_before_execution':True})
    assert candidate.source_identity()==frozen
    write_report('reports/native-details-metadata-guards.json',{'kind':'native_details_internal_member_negative_guards','database':db,'passed':True,'candidate_hook_sha256':frozen,'cases':checks,'all_metadata_changes_rolled_back':True,'user_recipe_function_validator_unchanged':True,'official_eligibility_unproven':True})
    print('Details member guards:',len(checks),'metadata alterations rejected before C invocation',flush=True)


if __name__=='__main__':main()
