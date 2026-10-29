"""Compact STABLE timestamp normalization with per-call numeric reuse and guarded native table1 COPY.

All original recipe validation remains. Native identities, receipts, indexes,
full FK checks, commit and versioning still run. Original SQL/COPY fallback
is retained for unavailable software, unsupported inputs or receiving layouts.
"""
import hashlib
import json
from pathlib import Path
from bench import native_fk_current_probe as base,native_fk_probe as fk,native_details_probe as details,native_compact_probe as compact,native_c_lab as lab
from bench.common import ROOT


def package(kind='main'):
    name='native-details-memo-trusted-package.json' if kind=='memo' else 'native-'+('stamp12' if kind=='stamp' else kind)+'-trusted-package.json'
    info=json.loads((ROOT/'reports'/name).read_text())
    source=(ROOT/info['source']).resolve();assert source.is_relative_to(ROOT/'native')
    assert hashlib.sha256(source.read_bytes()).hexdigest()==info['source_sha256']
    for item in info['files']:
        path=(ROOT/item['path']).resolve();assert path.is_relative_to(ROOT/'.runtime/postgresql/pgsql')
        assert hashlib.sha256(path.read_bytes()).hexdigest()==item['sha256']
    for item in info['vendor_sources']['files']:assert hashlib.sha256((ROOT/item['path']).read_bytes()).hexdigest()==item['sha256']
    return info


def source_identity():
    from bench import native_minimal_probe,native_compact_sql,native_compile_cache_probe
    modules=(base,fk,details,compact,lab,native_minimal_probe,native_compact_sql,native_compile_cache_probe)
    body=compact.base.source_identity()+''.join(Path(m.__file__).read_text(encoding='utf8') for m in modules)+Path(__file__).read_text(encoding='utf8')
    software=[package('stamp'),package(),base.package(),package('memo')]
    return hashlib.sha256((body+json.dumps(software,sort_keys=True)).encode()).hexdigest()


def main_member_guard():
    from utils import sql_literal as lit
    old=details.package();new=package()
    oldlib=next(Path(x['path']).stem for x in old['files'] if x['path'].endswith('.dll'))
    newlib=next(Path(x['path']).stem for x in new['files'] if x['path'].endswith('.dll'))
    body=details.member_guard()
    replacements=[(lit(old['extension']),lit(new['extension'])),(lit('$libdir/'+oldlib),lit('$libdir/'+newlib)),
        ("'details_load'","'main_load'"),('challenge_details_ingest','challenge_main_ingest'),("'details_identity'","'main_identity'"),('challenge_details_identity','challenge_main_identity'),
        ("'bigint[]'::regtype,6,","'bigint'::regtype,3,"),('wanted.args=6','wanted.args=3'),("::text||' ',5)","::text||' ',2)")]
    for before,after in replacements:assert before in body,before;body=body.replace(before,after)
    return body


def stamp_member_guard(original,info):
    body=original(info)
    assert body.count("p.provolatile<>'i'")==1
    return body.replace('challenge_compact12','challenge_compact_stamp12').replace("p.provolatile<>'i'","p.provolatile::text<>(CASE WHEN wanted.name='normalize' THEN 's' ELSE 'i' END)")


def data_aggregate():
    return """coalesce(bool_and(pipeline_domain_safe AND
      (text_column_1 IS NULL OR octet_length(text_column_1)<=128) AND
      (text_column_2 IS NULL OR NOT(text_column_2 ~ '^[A-Z]{2}$') OR
        (octet_length(text_column_2)=2 AND ascii(left(text_column_2,1)) BETWEEN 65 AND 90 AND ascii(right(text_column_2,1)) BETWEEN 65 AND 90))),true)
      AND current_setting('server_encoding')='UTF8'
      AND (SELECT collisdeterministic FROM pg_collation WHERE oid='default'::regcollation)"""


def addon_sql():
    from utils import sql_literal as lit
    info=package();ext=info['extension']
    return """DO $main_install$ DECLARE main_usable boolean:=false; main_data_safe boolean; BEGIN
      IF EXISTS(SELECT 1 FROM pg_temp.pipeline_native_extension_state)
        AND EXISTS(SELECT 1 FROM pg_available_extension_versions WHERE name="""+lit(ext)+' AND version='+lit(info['version'])+""" AND trusted)
        AND NOT EXISTS(SELECT 1 FROM pg_extension WHERE extname="""+lit(ext)+""") THEN
        CREATE EXTENSION """+ext+""" WITH SCHEMA pgdm_pipeline_work;
        UPDATE pg_temp.pipeline_main_state SET extension_oid=(SELECT oid FROM pg_extension WHERE extname="""+lit(ext)+');\nmain_usable := '+main_member_guard()+";\nIF main_usable THEN main_usable := pgdm_pipeline_work.main_identity()="+lit(info['source_sha256'])+"; END IF;\n"+"""
        IF main_usable THEN
          SELECT data_safe INTO main_data_safe FROM pg_temp.pipeline_main_state;
          IF main_data_safe IS NULL THEN
            SELECT """+data_aggregate()+""" INTO main_data_safe FROM pgdm_pipeline_work.pipeline_normalized_input;
            UPDATE pg_temp.pipeline_main_state SET data_safe=main_data_safe;
          END IF;
          main_usable:=main_usable AND main_data_safe;
        END IF;
        IF NOT main_usable THEN DROP EXTENSION """+ext+"""; UPDATE pg_temp.pipeline_main_state SET extension_oid=NULL; END IF;
      END IF;
      UPDATE pg_temp.pipeline_main_state SET usable=main_usable;
    END $main_install$;"""


def install():
    details.package=lambda:package('memo')
    old_details_addon=details.addon_sql
    def shared_proofs():
        body=old_details_addon()
        declaration='DECLARE native_details_usable boolean:=false; BEGIN'
        needle=' INTO native_details_usable FROM pgdm_pipeline_work.pipeline_normalized_input;'
        assert body.count(declaration)==1 and body.count(needle)==1
        body=body.replace(declaration,'DECLARE native_details_usable boolean:=false; main_data_safe boolean; BEGIN',1)
        return body.replace(needle,','+data_aggregate()+' INTO native_details_usable,main_data_safe FROM pgdm_pipeline_work.pipeline_normalized_input;\nUPDATE pg_temp.pipeline_main_state SET data_safe=main_data_safe;',1)
    details.addon_sql=shared_proofs
    # Replace only the separately pinned internal normalizer contract. The
    # original untrusted recipe validator and schema guards remain unchanged.
    old_guard=compact.member_guard;old_projection=compact.projection
    compact.package=lambda:package('stamp')
    compact.member_guard=lambda info:stamp_member_guard(old_guard,info)
    definition=lab.compact_definition()
    def projected(scopes):
        body=old_projection(scopes);assert body.count(definition)==1
        return body.replace(definition,definition.replace('pipeline_stamp pg_catalog.text','pipeline_local_stamp pg_catalog.timestamp'),1)
    compact.projection=projected
    from bench import native_compact_sql as compiler
    old_statement=compiler.statement
    def statement(original,scopes,axes_function,relation='pg_temp.native_compact_case'):
        body=old_statement(original,scopes,axes_function,relation=relation)
        needle="source_row.pipeline_stamp::timestamptz AT TIME ZONE 'America/Sao_Paulo'"
        if body.startswith('INSERT INTO public.table1'):
            assert body.count(needle)==1;body=body.replace(needle,'source_row.pipeline_local_stamp',1)
        return body
    compiler.statement=statement
    base.install()
    import pipeline.parallel_normalization as parallel
    import pipeline.workspace as work
    import pipeline.native_copy as native
    old_prepare=work.prepare_sql;old_input=parallel.prepare_input_sql;old_cleanup=work.cleanup_sql
    old_receipts=native.prepare_receipts_sql;old_copy=native.copy_sql
    work.prepare_sql=lambda scopes:"CREATE TEMP TABLE pipeline_main_state(usable boolean NOT NULL,extension_oid oid,affected bigint,previous_work_mem text,data_safe boolean) ON COMMIT DROP; INSERT INTO pg_temp.pipeline_main_state(usable) VALUES(false);\n"+old_prepare(scopes)
    parallel.prepare_input_sql=lambda scopes,logged=False:old_input(scopes,logged=logged)+('\n'+addon_sql() if logged else '')
    def receipts():
        body=old_receipts();needle='SELECT safe AS pipeline_copy_safe,'
        assert body.count(needle)==1
        return body.replace(needle,needle+'safe AND (SELECT usable FROM pg_temp.pipeline_main_state) AS pipeline_main_ready,',1)
    native.prepare_receipts_sql=receipts
    def copied(statement,table):
        original=old_copy(statement,table)
        if table!='table1':return original
        return """SELECT :'pipeline_main_ready'::boolean AND (SELECT count(*)<=128::bigint*1024*1024/512 FROM public.table3) AS pipeline_main_map_ready
\gset
\if :pipeline_main_map_ready
UPDATE pg_temp.pipeline_main_state SET previous_work_mem=current_setting('work_mem');
SET LOCAL work_mem='128MB';
UPDATE pg_temp.pipeline_main_state SET affected=pgdm_pipeline_work.main_load('pgdm_pipeline_work.pipeline_normalized_input','public.table1','public.table3');
SELECT pg_catalog.set_config('work_mem',previous_work_mem,true) FROM pg_temp.pipeline_main_state;
INSERT INTO pg_temp.pipeline_copy_receipts SELECT 'table1',affected FROM pg_temp.pipeline_main_state;
\else
"""+original+"""
\endif
"""
    native.copy_sql=copied
    info=package();ext=info['extension']
    def cleanup():
        return """SELECT coalesce(affected::text,'fallback') AS pipeline_main_affected FROM pg_temp.pipeline_main_state
\gset
\echo PGDM_TYPED_MAIN_AFFECTED=:pipeline_main_affected
DO $main_cleanup$ BEGIN IF EXISTS(SELECT 1 FROM pg_temp.pipeline_main_state WHERE extension_oid IS NOT NULL) THEN
  IF NOT EXISTS(SELECT 1 FROM pg_extension e JOIN pg_temp.pipeline_main_state s ON s.extension_oid=e.oid WHERE e.extname='"""+ext+"""' AND e.extnamespace='pgdm_pipeline_work'::regnamespace) THEN RAISE EXCEPTION 'Main extension cleanup identity changed'; END IF;
  DROP EXTENSION """+ext+"""; END IF; END $main_cleanup$;
"""+old_cleanup()
    work.cleanup_sql=cleanup
    from bench import server_copy_probe as server
    old_run=server.ORIGINAL_RUN;old_server_run=server.run
    def observed(*args,**kwargs):
        import re
        result=old_run(*args,**kwargs)
        matches=re.findall(r'^PGDM_TYPED_MAIN_AFFECTED=(fallback|[0-9]+)\s*$',result.stdout,re.M)
        assert result.returncode!=0 or len(matches)<=1
        result.typed_main_affected=matches[0] if matches else None
        result.stdout=re.sub(r'^PGDM_TYPED_MAIN_AFFECTED=(?:fallback|[0-9]+)\r?\n','',result.stdout,flags=re.M)
        return result
    server.ORIGINAL_RUN=observed
    def diagnosed(*args,**kwargs):
        result=old_server_run(*args,**kwargs)
        result.local_backend_diagnostics['typed_main_affected']=getattr(result,'typed_main_affected',None)
        return result
    server.run=diagnosed


def main():
    frozen=source_identity();install()
    from bench import service_run as driver
    original=driver.write_report
    def recorded(path,value):
        assert source_identity()==frozen
        original(path,{**value,'native_main_hook_sha256':frozen,'trusted_main_package':package(),'trusted_stamp_package':package('stamp'),'trusted_details_package':package('memo'),'official_eligibility_unproven':True})
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
