"""A DB owner can use/drop the pure native package, not replace its code."""
import hashlib
import json
import uuid
from psycopg import sql
import psycopg
from bench import native_c_lab as lab
from bench.common import ROOT,write_report


def main():
    package=json.loads((ROOT/'reports/native-c-trusted-package-v2.json').read_text())
    for record in package['files']:assert hashlib.sha256((ROOT/record['path']).read_bytes()).hexdigest()==record['sha256']
    role='native_owner_'+uuid.uuid4().hex[:8];database='lab_native_owner_'+uuid.uuid4().hex[:8]
    with lab.connect('postgres',autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE ROLE {} LOGIN CREATEDB NOSUPERUSER').format(sql.Identifier(role)))
        admin.execute(sql.SQL('CREATE DATABASE {} OWNER {}').format(sql.Identifier(database),sql.Identifier(role)))
    with psycopg.connect(host='127.0.0.1',port=lab.PORT,user=role,dbname=database,autocommit=True) as c:
        assert c.execute('SELECT rolsuper FROM pg_roles WHERE rolname=current_user').fetchone()==(False,)
        with c.transaction():
            c.execute('CREATE SCHEMA pgdm_pipeline_work')
            c.execute(sql.SQL('CREATE EXTENSION {} WITH SCHEMA pgdm_pipeline_work').format(sql.Identifier(package['extension'])))
            assert c.execute('SELECT pgdm_pipeline_work.identity()').fetchone()[0]==package['source_sha256']
            r=c.execute("SELECT n.pipeline_domain_safe,n.pipeline_identifier,n.pipeline_numeric_1,n.pipeline_axis_rows FROM pgdm_pipeline_work.normalize(%s::jsonb) n("+lab.compact_definition()+')',('{"id_column_1":"17","numeric_column_1":"0.001","integer_column_2":"1","text_column_4":" x ","numeric_column_3":"1.250"}',)).fetchone()
            assert r==(True,17,1,1),r
            assert c.execute("SELECT * FROM pgdm_pipeline_work.axes((SELECT pipeline_axes FROM pgdm_pipeline_work.normalize('{}'::jsonb) n("+lab.compact_definition()+')))').fetchall()==[]
            rejected=[]
            for query in ["ALTER FUNCTION pgdm_pipeline_work.normalize(jsonb) VOLATILE","CREATE FUNCTION pgdm_pipeline_work.arbitrary() RETURNS integer AS '$libdir/does_not_exist','arbitrary' LANGUAGE C"]:
                try:
                    with c.transaction():c.execute(query)
                except psycopg.Error as error:assert error.sqlstate=='42501',error;rejected.append(error.sqlstate)
                else:raise AssertionError('Unprivileged role changed trusted native code')
            members=c.execute("SELECT p.proname,p.provolatile,p.proparallel,p.prosecdef,r.rolsuper,l.lanname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_roles r ON r.oid=p.proowner JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname='pgdm_pipeline_work' ORDER BY p.proname").fetchall()
            assert len(members)==3 and all(row[1:]==('i','s',False,True,'c') for row in members)
            c.execute(sql.SQL('DROP EXTENSION {}').format(sql.Identifier(package['extension'])))
            c.execute('DROP SCHEMA pgdm_pipeline_work')
        assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()==(None,)
        assert not c.execute('SELECT 1 FROM pg_extension WHERE extname=%s',(package['extension'],)).fetchone()
    write_report(ROOT/'reports/native-c-trusted-owner-test.json',{'kind':'isolated_trusted_native_package_permissions_and_cleanup','passed':True,'database':database,'role':role,'non_superuser_database_owner':True,'source_sha256':package['source_sha256'],'native_normalization_values_checked':True,'function_code_and_untrusted_language_creation_rejected':rejected,'immutable_parallel_safe_security_invoker_superuser_owned_members':members,'extension_and_namespace_removed_before_commit':True,'application_function_validator_unchanged':True,'application_integration':False,'eligibility_unproven':True})
    print('Trusted native package: non-superuser owner/immutable code/exact values/transactional cleanup PASS',flush=True)


if __name__=='__main__':main()
