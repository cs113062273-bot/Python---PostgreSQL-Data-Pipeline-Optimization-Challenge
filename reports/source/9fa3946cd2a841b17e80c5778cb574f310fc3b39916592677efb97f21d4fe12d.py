"""Non-superuser typed package use, code protection, permissions and cleanup."""
import uuid
import psycopg
from psycopg import sql
from bench import native_c_lab as lab,native_main_compact_probe as candidate,native_compact_probe as normalizer
from bench import native_stamp_compact_lab as stamp
from bench.fixture import generator
from bench.common import write_report
from bench.run import execute,prepared
from tests.equivalence.oracle import snapshot,assert_exact


def main():
    db='lab_details_owner_'+uuid.uuid4().hex[:8];owner='details_owner_'+uuid.uuid4().hex[:8];outsider='details_reader_'+uuid.uuid4().hex[:8]
    info=candidate.package();norm=candidate.package('stamp')
    with lab.connect('postgres',autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE ROLE {} LOGIN CREATEDB NOSUPERUSER').format(sql.Identifier(owner)))
        admin.execute(sql.SQL('CREATE ROLE {} NOLOGIN NOSUPERUSER').format(sql.Identifier(outsider)))
        admin.execute(sql.SQL('GRANT {} TO {}').format(sql.Identifier(outsider),sql.Identifier(owner)))
        admin.execute(sql.SQL('CREATE DATABASE {} OWNER {}').format(sql.Identifier(db),sql.Identifier(owner)))
    with psycopg.connect(host='127.0.0.1',port=lab.PORT,user=owner,dbname=db,autocommit=True) as c:
        gen=generator();c.execute(gen.SCHEMA_SQL)
        with c.cursor().copy('COPY public.raw_data(raw_id,raw_hash,raw_ingested_at,raw_schema,raw_tab,raw) FROM STDIN') as stream:
            for row in gen.iter_generated_rows(64):stream.write_row((row.raw_id,row.raw_hash,row.raw_ingested_at,row.raw_schema,row.raw_tab,row.raw_json))
        execute(c,'baseline',['group001']);snapshot(c)
        c.execute('TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY')
        with c.transaction():
            c.execute("SET LOCAL work_mem='64MB'; CREATE SCHEMA pgdm_pipeline_work")
            c.execute('CREATE EXTENSION '+norm['extension']+' WITH SCHEMA pgdm_pipeline_work')
            assert c.execute('SELECT '+candidate.stamp_member_guard(normalizer.member_guard,norm)).fetchone()==(True,)
            c.execute('CREATE EXTENSION '+info['extension']+' WITH SCHEMA pgdm_pipeline_work')
            assert c.execute('SELECT '+candidate.main_member_guard()).fetchone()==(True,)
            assert c.execute('SELECT pgdm_pipeline_work.main_identity()').fetchone()==(info['source_sha256'],)
            for recipe in prepared('baseline',['group001'])[:2]:c.execute(recipe['statements'][0])
            c.execute("CREATE TEMP TABLE main_input ON COMMIT DROP AS SELECT s.raw_id,n.* FROM public.raw_data s CROSS JOIN LATERAL pgdm_pipeline_work.normalize(s.raw->'values') n("+stamp.compact_definition()+')')
            c.execute(sql.SQL('GRANT USAGE ON SCHEMA pgdm_pipeline_work TO {}').format(sql.Identifier(outsider)))
            c.execute(sql.SQL('GRANT SELECT ON public.table3,pg_temp.main_input TO {}').format(sql.Identifier(outsider)))
            denied=[]
            try:
                with c.transaction():
                    c.execute(sql.SQL('SET LOCAL ROLE {}').format(sql.Identifier(outsider)))
                    c.execute("SELECT pgdm_pipeline_work.main_load('pg_temp.main_input','public.table1','public.table3')")
            except psycopg.Error as error:assert error.sqlstate=='42501';denied.append({'case':'reader_without_target_insert','sqlstate':error.sqlstate})
            else:raise AssertionError('SECURITY INVOKER loader bypassed target INSERT permission')
            assert c.execute('SELECT count(*) FROM public.table1').fetchone()==(0,)
            counts=c.execute("SELECT pgdm_pipeline_work.main_load('pg_temp.main_input','public.table1','public.table3')").fetchone()[0]
            assert counts==c.execute('SELECT count(*) FROM public.table1').fetchone()[0]
            for recipe in prepared('baseline',['group001'])[3:]:c.execute(recipe['statements'][0])
            assert_exact(c)
            for query in ['ALTER FUNCTION pgdm_pipeline_work.main_load(regclass,regclass,regclass) SECURITY DEFINER',"CREATE FUNCTION pgdm_pipeline_work.arbitrary() RETURNS integer AS '$libdir/no_file','no_function' LANGUAGE C"]:
                try:
                    with c.transaction():c.execute(query)
                except psycopg.Error as error:assert error.sqlstate=='42501';denied.append({'case':'cannot_modify_or_add_untrusted_code','sqlstate':error.sqlstate})
                else:raise AssertionError('Non-superuser modified trusted C implementation')
            c.execute('DROP EXTENSION '+info['extension']+'; DROP EXTENSION '+norm['extension']+'; DROP SCHEMA pgdm_pipeline_work')
        assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()==(None,)
    write_report('reports/native-main-compact-owner-permissions.json',{'kind':'non_superuser_native_main_permissions_and_exact_outputs','passed':True,'database':db,'owner':owner,'reader':outsider,'source_sha256':info['source_sha256'],'six_canonical_outputs_exact':True,'native_counts':counts,'rejected':denied,'code_superuser_owned_security_invoker':True,'extension_and_namespace_removed_before_commit':True,'application_user_recipe_validator_unchanged':True,'official_eligibility_unproven':True})
    print('Main trusted package: ordinary owner exact native COPY / reader ACL / code protection / cleanup PASS',flush=True)


if __name__=='__main__':main()
