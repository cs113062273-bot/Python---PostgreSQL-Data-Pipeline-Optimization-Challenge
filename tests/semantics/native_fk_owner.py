"""Ordinary owner completion, reader rejection and pinned function guards."""
import uuid
import psycopg
from psycopg import sql
from bench import native_c_lab as lab,native_fk_probe as candidate
from bench.common import ROOT,write_report
from bench.fixture import generator
from bench.run import execute
from bench.native_fk_scan_component import TABLES


def main(*,output='reports/native-fk-owner-permissions.json'):
    output=(ROOT/output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    db='lab_fk_owner_'+uuid.uuid4().hex[:8];owner='fk_owner_'+uuid.uuid4().hex[:8];reader='fk_reader_'+uuid.uuid4().hex[:8]
    info=candidate.package();ext=info['extension'];checks=[]
    signature='pgdm_pipeline_work.fk_validate('+','.join(['regclass']*7)+')'
    call='SELECT pgdm_pipeline_work.fk_validate('+','.join("'public."+t+"'::regclass" for t in TABLES)+')'
    with lab.connect('postgres',autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE ROLE {} LOGIN CREATEDB NOSUPERUSER').format(sql.Identifier(owner)))
        admin.execute(sql.SQL('CREATE ROLE {} NOLOGIN NOSUPERUSER').format(sql.Identifier(reader)))
        admin.execute(sql.SQL('GRANT {} TO {}').format(sql.Identifier(reader),sql.Identifier(owner)))
        admin.execute(sql.SQL('CREATE DATABASE {} OWNER {}').format(sql.Identifier(db),sql.Identifier(owner)))
    with psycopg.connect(host='127.0.0.1',port=lab.PORT,user=owner,dbname=db,autocommit=True) as c:
        gen=generator();c.execute(gen.SCHEMA_SQL)
        with c.cursor().copy('COPY public.raw_data(raw_id,raw_hash,raw_ingested_at,raw_schema,raw_tab,raw) FROM STDIN') as stream:
            for row in gen.iter_generated_rows(64):stream.write_row((row.raw_id,row.raw_hash,row.raw_ingested_at,row.raw_schema,row.raw_tab,row.raw_json))
        execute(c,'baseline',['group001'])
        definitions=c.execute("SELECT conrelid::regclass::text,conname,pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace='public'::regnamespace AND contype='f' ORDER BY conrelid,conname").fetchall();assert len(definitions)==6
        for table,name,definition in definitions:
            c.execute(sql.SQL('ALTER TABLE {} DROP CONSTRAINT {}').format(sql.Identifier('public',table),sql.Identifier(name)))
            c.execute(sql.SQL('ALTER TABLE {} ADD CONSTRAINT {} '+definition+' NOT VALID').format(sql.Identifier('public',table),sql.Identifier(name)))
        c.execute('CREATE SCHEMA pgdm_pipeline_work; CREATE EXTENSION '+ext+' WITH SCHEMA pgdm_pipeline_work')
        assert c.execute('SELECT '+candidate.member_guard()).fetchone()==(True,)
        c.execute(sql.SQL('GRANT USAGE ON SCHEMA pgdm_pipeline_work TO {}').format(sql.Identifier(reader)))
        c.execute(sql.SQL('GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}').format(sql.Identifier(reader)))
        with c.transaction(force_rollback=True):
            c.execute(sql.SQL('SET LOCAL ROLE {}').format(sql.Identifier(reader)))
            assert c.execute(call).fetchone()==(None,)
            assert c.execute("SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace AND contype='f' AND convalidated").fetchone()==(0,)
        checks.append({'case':'SELECT_reader_cannot_complete_any_catalog_flags'})
        counts=c.execute(call).fetchone()[0];assert len(counts)==6
        assert c.execute("SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace AND contype='f' AND convalidated").fetchone()==(6,)
        checks.append({'case':'ordinary_owner_completes_all_actual_relationships','actual_checked_keys':counts})
        for query in ['ALTER FUNCTION '+signature+' SECURITY DEFINER',"CREATE FUNCTION pgdm_pipeline_work.arbitrary() RETURNS integer AS '$libdir/no_file','no_function' LANGUAGE C"]:
            try:
                with c.transaction(force_rollback=True):c.execute(query)
            except psycopg.Error as error:assert error.sqlstate=='42501'
            else:raise AssertionError('Owner changed superuser-owned C implementation')
            checks.append({'case':'untrusted_C_code_modification_rejected','sqlstate':'42501'})
        c.execute('DROP EXTENSION '+ext+'; DROP SCHEMA pgdm_pipeline_work')
    mutations=[('volatility','ALTER FUNCTION '+signature+' IMMUTABLE'),('parallel','ALTER FUNCTION '+signature+' PARALLEL SAFE'),
        ('security','ALTER FUNCTION '+signature+' SECURITY DEFINER'),('strict','ALTER FUNCTION '+signature+' CALLED ON NULL INPUT'),
        ('configuration','ALTER FUNCTION '+signature+' SET search_path TO public'),('membership','ALTER EXTENSION '+ext+' DROP FUNCTION '+signature),
        ('renamed','ALTER FUNCTION '+signature+' RENAME TO changed_fk'),('identity_volatility','ALTER FUNCTION pgdm_pipeline_work.fk_identity() VOLATILE')]
    with lab.connect(db,autocommit=True) as c:
        for name,change in mutations:
            with c.transaction(force_rollback=True):
                c.execute('CREATE SCHEMA pgdm_pipeline_work; CREATE EXTENSION '+ext+' WITH SCHEMA pgdm_pipeline_work')
                assert c.execute('SELECT '+candidate.member_guard()).fetchone()==(True,)
                c.execute(change);assert c.execute('SELECT '+candidate.member_guard()).fetchone()==(False,),name
                checks.append({'case':'altered_metadata_rejected_before_C','mutation':name})
    write_report(output,{'kind':'native_full_fk_validation_owner_permissions_and_guards','passed':True,'database':db,'owner':owner,'reader':reader,'source_sha256':info['source_sha256'],'checks':checks,'normal_RI_triggers_retained':True,'original_recipe_validator_unchanged':True,'official_eligibility_unproven':True})
    print(len(checks),'FK owner/reader/code protection/metadata checks PASS',flush=True)


if __name__=='__main__':main()
