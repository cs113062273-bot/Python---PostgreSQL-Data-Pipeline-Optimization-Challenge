"""Opt-in full native FK graph validation with original validation fallback.

All six normal RI trigger sets exist before scanning. Only after every actual
child reference has passed does C finish constraint validation in the same
transaction. Unsupported metadata or a wide key range runs native VALIDATE.
"""
import hashlib
import json
from pathlib import Path

from bench import native_details_probe as base,native_compile_cache_probe as cache
from bench.common import ROOT


def package():
    info=json.loads((ROOT/'reports/native-fk-trusted-package.json').read_text())
    assert hashlib.sha256((ROOT/'native/challenge_fk_validate.c').read_bytes()).hexdigest()==info['source_sha256']
    for item in info['files']:
        path=(ROOT/item['path']).resolve();assert path.is_relative_to(ROOT/'.runtime/postgresql/pgsql')
        assert hashlib.sha256(path.read_bytes()).hexdigest()==item['sha256']
    return info


def source_identity():
    return hashlib.sha256((base.source_identity()+Path(__file__).read_text(encoding='utf8')+Path(cache.__file__).read_text(encoding='utf8')+json.dumps(package(),sort_keys=True)).encode()).hexdigest()


def member_guard():
    # Same pinned invoker/catalog checks as the existing details extension,
    # specialized to this independent package's seven relation arguments.
    from utils import sql_literal as lit
    old=base.package();new=package()
    oldlib=next(Path(x['path']).stem for x in old['files'] if x['path'].endswith('.dll'))
    newlib=next(Path(x['path']).stem for x in new['files'] if x['path'].endswith('.dll'))
    body=base.member_guard()
    for before,after in [(lit(old['extension']),lit(new['extension'])),(lit('$libdir/'+oldlib),lit('$libdir/'+newlib)),
                         ("'details_load'","'fk_validate'"),('challenge_details_ingest','challenge_fk_validate'),("'details_identity'","'fk_identity'"),('challenge_details_identity','challenge_fk_identity'),
                         ("'bigint[]'::regtype,6,","'bigint[]'::regtype,7,"),('wanted.args=6','wanted.args=7'),("::text||' ',5)","::text||' ',6)")]:
        assert before in body,before;body=body.replace(before,after)
    return body


def addon_sql():
    from utils import sql_literal as lit
    info=package();ext=info['extension']
    return """DO $fk_install$ DECLARE native_fk_usable boolean:=false; BEGIN
      IF EXISTS(SELECT 1 FROM pg_temp.pipeline_native_extension_state)
        AND EXISTS(SELECT 1 FROM pg_available_extension_versions WHERE name="""+lit(ext)+' AND version='+lit(info['version'])+""" AND trusted)
        AND NOT EXISTS(SELECT 1 FROM pg_extension WHERE extname="""+lit(ext)+""") THEN
        CREATE EXTENSION """+ext+""" WITH SCHEMA pgdm_pipeline_work;
        UPDATE pg_temp.pipeline_fk_state SET extension_oid=(SELECT oid FROM pg_extension WHERE extname="""+lit(ext)+');\nnative_fk_usable := '+member_guard()+";\nIF native_fk_usable THEN native_fk_usable := pgdm_pipeline_work.fk_identity()="+lit(info['source_sha256'])+"; END IF;\n"+"""
        IF NOT native_fk_usable THEN DROP EXTENSION """+ext+"""; UPDATE pg_temp.pipeline_fk_state SET extension_oid=NULL; END IF;
      END IF;
      UPDATE pg_temp.pipeline_fk_state SET usable=native_fk_usable;
    END $fk_install$;"""


def restore_sql(original):
    loop=""" FOR item IN SELECT * FROM pg_temp.pipeline_bulk_foreign_keys ORDER BY table_name,constraint_name LOOP
   EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I %s',item.table_name,item.constraint_name,item.definition);
 END LOOP;"""
    assert original.count(loop)==1
    return original.replace(loop,""" IF (SELECT usable FROM pg_temp.pipeline_fk_state) THEN
   FOR item IN SELECT * FROM pg_temp.pipeline_bulk_foreign_keys ORDER BY table_name,constraint_name LOOP
     EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I %s NOT VALID',item.table_name,item.constraint_name,item.definition);
   END LOOP;
   UPDATE pg_temp.pipeline_fk_state SET previous_work_mem=current_setting('work_mem');
   PERFORM set_config('work_mem','128MB',true);
   UPDATE pg_temp.pipeline_fk_state SET counts=pgdm_pipeline_work.fk_validate('public.raw_data','public.table1','public.table3','public.table4','public.table2','public.table5','public.table6');
   PERFORM set_config('work_mem',previous_work_mem,true) FROM pg_temp.pipeline_fk_state;
   IF (SELECT counts IS NULL FROM pg_temp.pipeline_fk_state) THEN
     FOR item IN SELECT * FROM pg_temp.pipeline_bulk_foreign_keys ORDER BY table_name,constraint_name LOOP
       EXECUTE format('ALTER TABLE public.%I VALIDATE CONSTRAINT %I',item.table_name,item.constraint_name);
     END LOOP;
   END IF;
 ELSE
"""+loop+"""
 END IF;""")


def install():
    base.install();cache.install_cache()
    import pipeline.parallel_normalization as parallel
    import pipeline.workspace as work
    import pipeline.bulk_fks as fks
    from bench import server_copy_probe as server
    old_prepare=work.prepare_sql;old_projection=parallel.prepare_input_sql;old_cleanup=work.cleanup_sql
    old_restore=fks.restore_sql;old_run=server.ORIGINAL_RUN
    work.prepare_sql=lambda scopes:"CREATE TEMP TABLE pipeline_fk_state(usable boolean NOT NULL,extension_oid oid,counts bigint[],previous_work_mem text) ON COMMIT DROP; INSERT INTO pg_temp.pipeline_fk_state(usable) VALUES(false);\n"+old_prepare(scopes)
    parallel.prepare_input_sql=lambda scopes,logged=False:old_projection(scopes,logged=logged)+('\n'+addon_sql() if logged else '')
    fks.restore_sql=lambda:restore_sql(old_restore())
    info=package();ext=info['extension']
    def cleanup():
        # A transport receipt verifies that the new function actually ran;
        # fallback and unsupported domain must never produce a ranked claim.
        return """SELECT coalesce(array_to_string(counts,','),'fallback') AS pipeline_fk_checked FROM pg_temp.pipeline_fk_state
\\gset
\\echo PGDM_SHARED_FK_CHECKED=:pipeline_fk_checked
DO $fk_cleanup$ BEGIN IF EXISTS(SELECT 1 FROM pg_temp.pipeline_fk_state WHERE extension_oid IS NOT NULL) THEN
  IF NOT EXISTS(SELECT 1 FROM pg_extension e JOIN pg_temp.pipeline_fk_state s ON s.extension_oid=e.oid WHERE e.extname='"""+ext+"""' AND e.extnamespace='pgdm_pipeline_work'::regnamespace) THEN RAISE EXCEPTION 'FK extension cleanup identity changed'; END IF;
  DROP EXTENSION """+ext+"""; END IF; END $fk_cleanup$;
"""+old_cleanup()
    work.cleanup_sql=cleanup
    def observed(*args,**kwargs):
        import re
        result=old_run(*args,**kwargs)
        matches=re.findall(r'^PGDM_SHARED_FK_CHECKED=(fallback|[0-9]+(?:,[0-9]+){5})\s*$',result.stdout,re.M)
        assert result.returncode!=0 or len(matches)<=1
        result.shared_fk_checked=matches[0] if matches else None
        result.stdout=re.sub(r'^PGDM_SHARED_FK_CHECKED=(?:fallback|[0-9]+(?:,[0-9]+){5})\r?\n','',result.stdout,flags=re.M)
        return result
    server.ORIGINAL_RUN=observed
    original_server_run=server.run
    def diagnosed(*args,**kwargs):
        result=original_server_run(*args,**kwargs)
        result.local_backend_diagnostics['shared_fk_checked']=getattr(result,'shared_fk_checked',None)
        return result
    server.run=diagnosed


def main():
    frozen=source_identity();install()
    from bench import service_run as driver
    original=driver.write_report
    def recorded(path,value):
        assert source_identity()==frozen
        original(path,{**value,'native_fk_hook_sha256':frozen,'trusted_fk_package':package(),'official_eligibility_unproven':True})
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
