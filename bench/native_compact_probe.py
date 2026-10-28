"""Transient trusted pure-argument implementation, original user SQL guards.

Research only. Package/software metadata and exact immutable C entry points are
checked separately from the unchanged untrusted recipe policy. The extension
is created in the transaction-owned work namespace and removed before commit.
Unavailable/conflicting implementations select the complete original program.
"""
import hashlib
import json
from pathlib import Path
from bench import slim_inline_probe as base,native_compact_sql as compact,native_c_lab as lab
from bench.common import ROOT,write_report
from pipeline.normalization import source_sql


def package():
    info=json.loads((ROOT/'reports/native-c-trusted-package-v2.json').read_text())
    assert hashlib.sha256((ROOT/'native/challenge_probe.c').read_bytes()).hexdigest()==info['source_sha256']
    for item in info['files']:
        path=(ROOT/item['path']).resolve();assert path.is_relative_to(ROOT/'.runtime/postgresql/pgsql')
        assert hashlib.sha256(path.read_bytes()).hexdigest()==item['sha256']
    return info


def source_identity():
    info=package()
    return hashlib.sha256((base.source_identity()+Path(compact.__file__).read_text(encoding='utf8')+Path(lab.__file__).read_text(encoding='utf8')+Path(__file__).read_text(encoding='utf8')+info['source_sha256']+json.dumps(info['files'],sort_keys=True)).encode()).hexdigest()


def retag(program,marker):
    opening='DO '+marker+' ';closing='END '+marker+';'
    start=program.index(opening);end=program.rindex(closing)
    tag='$native_'+hashlib.sha256(program.encode()).hexdigest()[:20]+'$'
    while tag in program:tag=tag[:-1]+'_x$'
    return program[:start]+'DO '+tag+' '+program[start+len(opening):end]+'END '+tag+';'+program[end+len(closing):]


def member_guard(info):
    from utils import sql_literal
    extension=sql_literal(info['extension']);version=sql_literal(info['version'])
    library=next(Path(item['path']).stem for item in info['files'] if item['path'].endswith('.dll'))
    binary=sql_literal('$libdir/'+library)
    return """EXISTS(SELECT 1 FROM pg_catalog.pg_extension e
      WHERE e.extname="""+extension+' AND e.extversion='+version+""" AND e.extnamespace='pgdm_pipeline_work'::regnamespace
      AND e.extowner=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user))
      AND (SELECT count(*) FROM pg_catalog.pg_proc p WHERE p.pronamespace='pgdm_pipeline_work'::regnamespace)=3
      AND NOT EXISTS(SELECT 1 FROM (VALUES
        ('normalize','challenge_compact12','record'::regtype,false,1,'jsonb'::regtype),
        ('axes','challenge_axis_rows','record'::regtype,true,1,'bytea'::regtype),
        ('identity','challenge_probe_identity','text'::regtype,false,0,0::oid))
        wanted(name,source,result,returns_set,args,argtype)
        LEFT JOIN pg_catalog.pg_proc p ON p.pronamespace='pgdm_pipeline_work'::regnamespace AND p.proname=wanted.name
        LEFT JOIN pg_catalog.pg_language l ON l.oid=p.prolang
        LEFT JOIN pg_catalog.pg_roles owner ON owner.oid=p.proowner
        WHERE p.oid IS NULL OR l.lanname<>'c' OR NOT owner.rolsuper OR p.provolatile<>'i' OR p.proparallel<>'s'
        OR p.prosecdef OR p.proconfig IS NOT NULL OR p.prosupport<>0 OR p.prokind<>'f'
        OR p.prosrc<>wanted.source OR p.probin<>"""+binary+""" OR p.prorettype<>wanted.result
        OR p.proretset<>wanted.returns_set OR p.pronargs<>wanted.args
        OR (wanted.args=1 AND p.proargtypes[0]<>wanted.argtype)
        OR NOT EXISTS(SELECT 1 FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_extension e ON e.oid=d.refobjid
          WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.refclassid='pg_extension'::regclass
          AND d.deptype='e' AND e.extname="""+extension+'))'


def projection(scopes):
    from utils import sql_literal
    info=package();extension=info['extension'];assert all(ch.isalnum() or ch=='_' for ch in extension)
    select='SELECT s.raw_id,n.* FROM '+source_sql(scopes)+" s CROSS JOIN LATERAL pgdm_pipeline_work.normalize(s.raw->'values') n("+lab.compact_definition()+')'
    fallback='SELECT s.raw_id,false AS pipeline_domain_safe FROM '+source_sql(scopes)+' s'
    program='-- native compact source_sha256='+source_identity()+"\nDO $native_input$ DECLARE target text:='challenge_lab.pipeline_normalized_input'; usable boolean:=false; BEGIN\n"+"""CREATE TEMP TABLE pipeline_native_extension_state(extension_oid oid NOT NULL) ON COMMIT DROP;
      IF EXISTS(SELECT 1 FROM pg_catalog.pg_available_extension_versions WHERE name="""+sql_literal(extension)+' AND version='+sql_literal(info['version'])+""" AND trusted)
      AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_extension WHERE extname="""+sql_literal(extension)+') THEN\nCREATE EXTENSION '+extension+' WITH SCHEMA pgdm_pipeline_work;\n'+"INSERT INTO pg_temp.pipeline_native_extension_state SELECT oid FROM pg_catalog.pg_extension WHERE extname="+sql_literal(extension)+';\nusable := '+member_guard(info)+";\nIF usable THEN usable := pgdm_pipeline_work.identity()="+sql_literal(info['source_sha256'])+"; END IF;\nIF NOT usable THEN DROP EXTENSION "+extension+"; DELETE FROM pg_temp.pipeline_native_extension_state; END IF;\nEND IF;\n"+"IF usable THEN EXECUTE 'CREATE UNLOGGED TABLE '||target||' AS '||"+sql_literal(select)+"; ELSE EXECUTE 'CREATE UNLOGGED TABLE '||target||' AS '||"+sql_literal(fallback)+"; END IF;\nEND $native_input$;"
    return retag(program,'$native_input$')


def install():
    base.install()
    import pipeline.parallel_normalization as parallel
    import pipeline.guarded_normalization as guarded
    import pipeline.workspace as workspace
    old_projection=parallel.prepare_input_sql;old_statement=guarded.fast_statement;old_cleanup=workspace.cleanup_sql;old_prepare=workspace.prepare_sql
    workspace.prepare_sql=lambda scopes:retag(old_prepare(scopes),'$work_namespace$')
    parallel.prepare_input_sql=lambda scopes,logged=False:projection(scopes) if logged else old_projection(scopes,logged=logged)
    def transformed(original,scopes,mode=guarded.MODE):
        if mode=='guarded_copy':return compact.statement(original,scopes,'pgdm_pipeline_work.axes',relation=workspace.RELATION)
        return old_statement(original,scopes,mode)
    guarded.fast_statement=transformed
    info=package();extension=info['extension']
    def cleanup():
        return """DO $native_cleanup$ BEGIN
          IF pg_catalog.to_regclass('pg_temp.pipeline_native_extension_state') IS NOT NULL THEN
            IF EXISTS(SELECT 1 FROM pg_temp.pipeline_native_extension_state) THEN
              IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_extension e JOIN pg_temp.pipeline_native_extension_state s ON s.extension_oid=e.oid
                WHERE e.extname='"""+extension+"""' AND e.extnamespace='pgdm_pipeline_work'::regnamespace) THEN
                RAISE EXCEPTION 'Native extension cleanup identity changed'; END IF;
              DROP EXTENSION """+extension+""";
            END IF;
          END IF;
        END $native_cleanup$;
        """+old_cleanup()
    workspace.cleanup_sql=cleanup
    # The original mixin parses this stage's exact name to persist safe_domain
    # in version provenance. Keep its protocol name; describe the native
    # implementation in the separate hook/package fields instead.


def main():
    frozen=source_identity();install()
    import bench.service_run as driver
    original=driver.write_report
    def recorded(path,value):
        assert source_identity()==frozen
        value={**value,'native_compact_hook_sha256':frozen,'trusted_native_package':package(),'official_eligibility_unproven':True}
        if value.get('local_backend_diagnostics'):value['local_backend_diagnostics']['data_strategy']='native_compact'
        original(path,value)
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
