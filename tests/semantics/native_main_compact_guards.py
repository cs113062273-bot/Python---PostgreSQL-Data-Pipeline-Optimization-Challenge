"""Reject altered typed-loader and STABLE-normalizer metadata before use."""
import uuid
from bench import native_c_lab as lab,native_main_compact_probe as candidate,native_compact_probe as compact
from bench.common import ROOT,write_report


def main():
    output=ROOT/'reports/native-main-compact-metadata-guards.json';assert not output.exists()
    db='lab_main_guards_'+uuid.uuid4().hex[:8];lab.fixture(db);checks=[]
    signature='pgdm_pipeline_work.main_load(regclass,regclass,regclass)'
    info=candidate.package()
    main_cases=[('volatility','ALTER FUNCTION '+signature+' IMMUTABLE'),('parallel','ALTER FUNCTION '+signature+' PARALLEL SAFE'),
        ('security','ALTER FUNCTION '+signature+' SECURITY DEFINER'),('strict','ALTER FUNCTION '+signature+' CALLED ON NULL INPUT'),
        ('configuration','ALTER FUNCTION '+signature+' SET search_path TO public'),('membership','ALTER EXTENSION '+info['extension']+' DROP FUNCTION '+signature),
        ('renamed','ALTER FUNCTION '+signature+' RENAME TO changed_main'),('identity_volatility','ALTER FUNCTION pgdm_pipeline_work.main_identity() VOLATILE')]
    stamp=candidate.package('stamp')
    stamp_cases=[('incorrect_immutable','ALTER FUNCTION pgdm_pipeline_work.normalize(jsonb) IMMUTABLE'),('incorrect_volatile','ALTER FUNCTION pgdm_pipeline_work.normalize(jsonb) VOLATILE'),
        ('parallel_unsafe','ALTER FUNCTION pgdm_pipeline_work.normalize(jsonb) PARALLEL UNSAFE'),('security_definer','ALTER FUNCTION pgdm_pipeline_work.normalize(jsonb) SECURITY DEFINER'),
        ('configuration','ALTER FUNCTION pgdm_pipeline_work.normalize(jsonb) SET TimeZone TO UTC')]
    with lab.connect(db,autocommit=True) as c:
        for family,software,vectors in [('main',info,main_cases),('stamp',stamp,stamp_cases)]:
            for name,change in vectors:
                with c.transaction(force_rollback=True):
                    c.execute('CREATE SCHEMA pgdm_pipeline_work; CREATE EXTENSION '+software['extension']+' WITH SCHEMA pgdm_pipeline_work')
                    guard=candidate.main_member_guard() if family=='main' else candidate.stamp_member_guard(compact.member_guard,stamp)
                    assert c.execute('SELECT '+guard).fetchone()==(True,)
                    c.execute(change);assert c.execute('SELECT '+guard).fetchone()==(False,),(family,name)
                checks.append({'family':family,'mutation':name,'rejected_before_execution':True})
    write_report(output,{'kind':'typed_main_and_STABLE_stamp_member_guards','database':db,'passed':True,'checks':checks,'original_user_recipe_validator_unchanged':True,'all_mutations_rolled_back':True,'official_eligibility_unproven':True})
    print(len(checks),'typed main/STABLE timestamp metadata guards PASS',flush=True)


if __name__=='__main__':main()
