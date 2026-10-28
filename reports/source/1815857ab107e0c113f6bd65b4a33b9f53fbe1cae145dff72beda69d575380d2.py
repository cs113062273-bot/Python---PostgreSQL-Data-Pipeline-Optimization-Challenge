"""Guarded shared typed COPY for table5/6/2; exact original fallback retained.

The internal VOLATILE SECURITY INVOKER loader has its own pinned catalog and
software identity guard. The original user-recipe safety validator is unchanged.
"""
import hashlib
import json
from pathlib import Path
from bench import native_minimal_probe as base,native_compact_probe as compact
from bench.common import ROOT,write_report


def package():
    value=json.loads((ROOT/'reports/native-details-trusted-package.json').read_text())
    assert hashlib.sha256((ROOT/'native/challenge_details_ingest.c').read_bytes()).hexdigest()==value['source_sha256']
    for item in value['files']:
        path=(ROOT/item['path']).resolve();assert path.is_relative_to(ROOT/'.runtime/postgresql/pgsql')
        assert hashlib.sha256(path.read_bytes()).hexdigest()==item['sha256']
    for item in value['vendor_sources']['files']:assert hashlib.sha256((ROOT/item['path']).read_bytes()).hexdigest()==item['sha256']
    return value


def source_identity():
    return hashlib.sha256((base.source_identity()+Path(__file__).read_text(encoding='utf8')+json.dumps(package(),sort_keys=True)).encode()).hexdigest()


def member_guard():
    from utils import sql_literal as lit
    info=package();extension=lit(info['extension']);version=lit(info['version'])
    library=next(Path(x['path']).stem for x in info['files'] if x['path'].endswith('.dll'))
    return """EXISTS(SELECT 1 FROM pg_catalog.pg_extension e WHERE e.extname="""+extension+' AND e.extversion='+version+""" AND e.extnamespace='pgdm_pipeline_work'::regnamespace
      AND e.extowner=(SELECT oid FROM pg_roles WHERE rolname=current_user))
      AND (SELECT count(*) FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid WHERE d.refclassid='pg_extension'::regclass AND d.classid='pg_proc'::regclass AND d.deptype='e' AND e.extname="""+extension+""")=2
      AND NOT EXISTS(SELECT 1 FROM (VALUES
        ('details_load','challenge_details_ingest','bigint[]'::regtype,6,'v','u',true),
        ('details_identity','challenge_details_identity','text'::regtype,0,'i','s',false)) wanted(name,symbol,result,args,volatility,parallel,strict)
        LEFT JOIN pg_proc p ON p.pronamespace='pgdm_pipeline_work'::regnamespace AND p.proname=wanted.name
        LEFT JOIN pg_language l ON l.oid=p.prolang LEFT JOIN pg_roles r ON r.oid=p.proowner
        WHERE p.oid IS NULL OR l.lanname<>'c' OR NOT r.rolsuper OR p.prosecdef OR p.proretset
          OR p.provolatile::text<>wanted.volatility OR p.proparallel::text<>wanted.parallel OR p.proisstrict<>wanted.strict
          OR p.prosrc<>wanted.symbol OR p.probin<>"""+lit('$libdir/'+library)+""" OR p.prorettype<>wanted.result
          OR p.pronargs<>wanted.args OR (wanted.args=6 AND p.proargtypes::text<>repeat('regclass'::regtype::oid::text||' ',5)||'regclass'::regtype::oid::text)
          OR p.prokind<>'f' OR p.proconfig IS NOT NULL OR p.prosupport<>0
          OR NOT EXISTS(SELECT 1 FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.refclassid='pg_extension'::regclass AND d.deptype='e' AND e.extname="""+extension+'))'


def addon_sql():
    from utils import sql_literal as lit
    info=package();ext=info['extension']
    return """DO $details_install$ DECLARE native_details_usable boolean:=false; BEGIN
      IF EXISTS(SELECT 1 FROM pg_temp.pipeline_native_extension_state)
        AND EXISTS(SELECT 1 FROM pg_available_extension_versions WHERE name="""+lit(ext)+' AND version='+lit(info['version'])+""" AND trusted)
        AND NOT EXISTS(SELECT 1 FROM pg_extension WHERE extname="""+lit(ext)+""" ) THEN
        CREATE EXTENSION """+ext+""" WITH SCHEMA pgdm_pipeline_work;
        UPDATE pg_temp.pipeline_details_state SET extension_oid=(SELECT oid FROM pg_extension WHERE extname="""+lit(ext)+');\nnative_details_usable := '+member_guard()+";\nIF native_details_usable THEN native_details_usable := pgdm_pipeline_work.details_identity()="+lit(info['source_sha256'])+"; END IF;\n"+"""
        IF native_details_usable THEN
          SELECT coalesce(bool_and(pipeline_domain_safe AND (text_column_3 IS NULL OR text_column_3='' OR text_column_3 ~ '^[A-Z]{2}$')),true)
            AND count(*)<=128::bigint*1024*1024/96 INTO native_details_usable FROM pgdm_pipeline_work.pipeline_normalized_input;
          native_details_usable := native_details_usable AND (SELECT collisdeterministic FROM pg_collation WHERE oid='default'::regcollation);
        END IF;
        IF NOT native_details_usable THEN DROP EXTENSION """+ext+"""; UPDATE pg_temp.pipeline_details_state SET extension_oid=NULL; END IF;
      END IF;
      UPDATE pg_temp.pipeline_details_state SET usable=native_details_usable;
    END $details_install$;"""


def install():
    base.install()
    import pipeline.parallel_normalization as parallel
    import pipeline.workspace as work
    import pipeline.native_copy as native
    import pipeline.bulk_indexes as indexes
    old_projection=parallel.prepare_input_sql;old_prepare=work.prepare_sql;old_cleanup=work.cleanup_sql
    old_receipts=native.prepare_receipts_sql;old_copy=native.copy_sql
    work.prepare_sql=lambda scopes:"CREATE TEMP TABLE pipeline_details_state(usable boolean NOT NULL,extension_oid oid,counts bigint[] CHECK(array_length(counts,1)=3),previous_work_mem text) ON COMMIT DROP; INSERT INTO pg_temp.pipeline_details_state(usable) VALUES(false);\n"+old_prepare(scopes)
    parallel.prepare_input_sql=lambda scopes,logged=False:old_projection(scopes,logged=logged)+('\n'+addon_sql() if logged else '')
    def receipts():
        body=old_receipts();needle='SELECT safe AS pipeline_copy_safe FROM pg_temp.pipeline_normalization_domain'
        assert body.count(needle)==1
        return body.replace(needle,'SELECT safe AS pipeline_copy_safe,safe AND (SELECT usable FROM pg_temp.pipeline_details_state) AS pipeline_details_ready FROM pg_temp.pipeline_normalization_domain',1)
    native.prepare_receipts_sql=receipts
    def copied(statement,table):
        original=old_copy(statement,table)
        if table not in ('table5','table6','table2'):return original
        typed=''
        if table=='table5':
            typed=indexes.prepare_mode_sql('guarded_copy','table6')+'\n'+indexes.prepare_mode_sql('guarded_copy','table2')+"""
UPDATE pg_temp.pipeline_details_state SET previous_work_mem=current_setting('work_mem');
SET LOCAL work_mem='128MB';
UPDATE pg_temp.pipeline_details_state SET counts=pgdm_pipeline_work.details_load('pgdm_pipeline_work.pipeline_normalized_input','public.table1','public.table5','public.table6','public.table2','public.table4');
SELECT pg_catalog.set_config('work_mem',previous_work_mem,true) FROM pg_temp.pipeline_details_state;
INSERT INTO pg_temp.pipeline_copy_receipts SELECT names.table_name,counts.affected FROM pg_temp.pipeline_details_state s
 CROSS JOIN LATERAL unnest(ARRAY['table5','table6','table2']::text[],s.counts) counts(table_name,affected)
 CROSS JOIN LATERAL (SELECT counts.table_name) names;
"""
        return '\\if :pipeline_details_ready\n'+typed+'\n\\else\n'+original+'\n\\endif\n'
    native.copy_sql=copied
    info=package();ext=info['extension']
    def cleanup():
        return """DO $details_cleanup$ BEGIN IF EXISTS(SELECT 1 FROM pg_temp.pipeline_details_state WHERE extension_oid IS NOT NULL) THEN
          IF NOT EXISTS(SELECT 1 FROM pg_extension e JOIN pg_temp.pipeline_details_state s ON s.extension_oid=e.oid
            WHERE e.extname='"""+ext+"""' AND e.extnamespace='pgdm_pipeline_work'::regnamespace) THEN RAISE EXCEPTION 'Details extension cleanup identity changed'; END IF;
          DROP EXTENSION """+ext+"""; END IF; END $details_cleanup$;
        """+old_cleanup()
    work.cleanup_sql=cleanup


def main():
    frozen=source_identity();install()
    import bench.service_run as driver
    original=driver.write_report
    def recorded(path,value):
        assert source_identity()==frozen
        value={**value,'native_details_hook_sha256':frozen,'trusted_details_package':package(),'official_eligibility_unproven':True}
        if value.get('local_backend_diagnostics'):value['local_backend_diagnostics']['data_strategy']='native_shared_typed_details'
        original(path,value)
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
