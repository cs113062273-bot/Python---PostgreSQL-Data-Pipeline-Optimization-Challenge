"""Negative native member metadata and actual full-original fallbacks."""
import os
from pathlib import Path
import uuid
from bench import native_c_lab as lab, native_compact_probe as candidate
from bench.common import ROOT, write_report
from bench.control import seed_initial_versions
from bench.run import execute
from pipeline.local_service import LocalService
from pipeline.bulk_fks import BulkFKMixin
from pipeline.bulk_indexes import BulkIndexMixin
from pipeline.guarded_normalization import GuardedNormalizationMixin
from pipeline.recipes import ORDER, destinations, bulk_unique_tables
from pipeline.upstream import PostgresAdminService
from tests.equivalence.oracle import snapshot, assert_exact, protected_schema_signature, sequence_signature
from tests.recovery.native_copy_atomic import source_sha


def main():
    os.environ['CHALLENGE_PSQL']=str(ROOT/'.runtime/postgresql/pgsql/bin/psql.exe')
    db='lab_native_guards_'+uuid.uuid4().hex[:8]
    lab.fixture(db);seed_initial_versions(db)
    info=candidate.package();extension=info['extension'];frozen=candidate.source_identity()
    mutations=[('volatility','ALTER FUNCTION pgdm_pipeline_work.normalize(jsonb) VOLATILE'),
               ('parallel','ALTER FUNCTION pgdm_pipeline_work.axes(bytea) PARALLEL UNSAFE'),
               ('security','ALTER FUNCTION pgdm_pipeline_work.normalize(jsonb) SECURITY DEFINER'),
               ('configuration',"ALTER FUNCTION pgdm_pipeline_work.identity() SET search_path TO public"),
               ('missing_member','ALTER FUNCTION pgdm_pipeline_work.axes(bytea) RENAME TO changed_axes'),
               ('non_member','ALTER EXTENSION '+extension+' DROP FUNCTION pgdm_pipeline_work.identity()'),
               ('wrong_source',"CREATE OR REPLACE FUNCTION pgdm_pipeline_work.identity() RETURNS text AS '$libdir/"+next(Path(f['path']).stem for f in info['files'] if f['path'].endswith('.dll'))+"','challenge_extract64' LANGUAGE C IMMUTABLE PARALLEL SAFE"),
               ('extra_member','CREATE FUNCTION pgdm_pipeline_work.extra() RETURNS integer LANGUAGE SQL IMMUTABLE AS $$SELECT 1$$')]
    checks=[]
    class RollbackCase(Exception):pass
    with lab.connect(db,autocommit=True) as c:
        for name,query in mutations:
            try:
                with c.transaction():
                    c.execute('CREATE SCHEMA pgdm_pipeline_work')
                    c.execute('CREATE EXTENSION '+extension+' WITH SCHEMA pgdm_pipeline_work')
                    assert c.execute('SELECT '+candidate.member_guard(info)).fetchone()==(True,)
                    c.execute(query)
                    assert c.execute('SELECT '+candidate.member_guard(info)).fetchone()==(False,),name
                    checks.append({'mutation':name,'guard_rejected':True})
                    raise RollbackCase()
            except RollbackCase:pass
            assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()==(None,)
        execute(c,'baseline',['group001']);snapshot(c)
        c.execute('TRUNCATE '+','.join('public.'+t for t in ORDER)+' RESTART IDENTITY')
        original_schema=protected_schema_signature(c);original_sequences=sequence_signature(c);source=source_sha(c)
        candidate.install()
        original_projection=candidate.projection
        class Service(BulkFKMixin,BulkIndexMixin,GuardedNormalizationMixin,LocalService):
            bulk_foreign_keys=guarded_normalization=True
            bulk_recipe_mode='guarded_copy'
            bulk_unique_tables=bulk_unique_tables('guarded_copy')
        fallbacks=[]
        for name in ('unavailable_catalog_injected','preexisting_extension','preexisting_work_namespace'):
            if name=='unavailable_catalog_injected':
                def unavailable(scopes):
                    program=original_projection(scopes)
                    assert program.count(' AND trusted)')==1
                    return program.replace(' AND trusted)',' AND trusted AND false)',1)
                candidate.projection=unavailable
            elif name=='preexisting_extension':
                c.execute('CREATE SCHEMA native_existing')
                c.execute('CREATE EXTENSION '+extension+' WITH SCHEMA native_existing')
                existing=c.execute('SELECT oid FROM pg_extension WHERE extname=%s',(extension,)).fetchone()[0]
            else:c.execute('CREATE SCHEMA pgdm_pipeline_work; CREATE TABLE pgdm_pipeline_work.user_owned(value integer); INSERT INTO pgdm_pipeline_work.user_owned VALUES(17)')
            service=Service()
            timing=PostgresAdminService.create_cross_table_expansion_timing_report(db,'public.raw_data',['group001'],[d['table_name'] for d in destinations()])
            result=service.execute_cross_table_expansion(db,'public.raw_data',['group001'],destinations('guarded_copy'),timing_report=timing)
            assert result['execution_architecture']['safe_domain'] is False,(name,result['execution_architecture'])
            assert_exact(c)
            assert protected_schema_signature(c)==original_schema and sequence_signature(c)==original_sequences
            assert source_sha(c)==source
            for table in ORDER:
                assert c.execute('SELECT count(*) FROM public.'+table).fetchone()[0]==c.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s",(table,)).fetchone()[0]
            if name=='preexisting_extension':
                assert c.execute('SELECT oid FROM pg_extension WHERE extname=%s',(extension,)).fetchone()==(existing,)
                assert c.execute('SELECT native_existing.identity()').fetchone()==(info['source_sha256'],)
                c.execute('DROP EXTENSION '+extension+'; DROP SCHEMA native_existing')
            elif name=='preexisting_work_namespace':
                assert c.execute('SELECT * FROM pgdm_pipeline_work.user_owned').fetchall()==[(17,)]
                c.execute('DROP TABLE pgdm_pipeline_work.user_owned; DROP SCHEMA pgdm_pipeline_work')
            candidate.projection=original_projection
            c.execute('TRUNCATE '+','.join('public.'+t for t in ORDER)+' RESTART IDENTITY')
            c.execute("UPDATE public.pgdm_table_row_counts SET row_count=0 WHERE schema_name='public'")
            fallbacks.append({'case':name,'complete_original_outputs':True,'safe_domain':False,'unowned_objects_preserved':True})
            print(name,'PASS',flush=True)
    assert candidate.source_identity()==frozen
    write_report('reports/semantics-native-compact-guards.json',{'kind':'trusted_native_metadata_negative_and_original_fallback','database':db,'passed':True,'candidate_hook_sha256':frozen,'metadata_mutations':checks,'full_service_fallbacks':fallbacks,'application_function_validator_unchanged':True,'official_eligibility_unproven':True,'limits':'Unavailable catalog is injected in captured implementation condition; no installed software or server catalogs are removed. Metadata mutants are isolated ordinary DDL rolled back per case. No version registration or timing score claimed here.'})


if __name__=='__main__':main()
