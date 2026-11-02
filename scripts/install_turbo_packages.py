"""Stage the pinned turbo normalizer and typed COPY packages."""
import hashlib
import json
import os
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from bench.common import write_report

BODIES={'stamp':"""CREATE FUNCTION @extschema@.normalize(pg_catalog.jsonb) RETURNS pg_catalog.record
 AS 'MODULE_PATHNAME','challenge_compact13' LANGUAGE C STABLE PARALLEL SAFE SECURITY INVOKER CALLED ON NULL INPUT;
CREATE FUNCTION @extschema@.axes(pg_catalog.bytea) RETURNS TABLE(position_number pg_catalog.int2,weight pg_catalog.int4,axis_kind pg_catalog.text,gap pg_catalog.int4)
 AS 'MODULE_PATHNAME','challenge_axis_rows' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER ROWS 14;
CREATE FUNCTION @extschema@.identity() RETURNS pg_catalog.text
 AS 'MODULE_PATHNAME','challenge_probe_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER;
""",'main':"""CREATE FUNCTION @extschema@.main_load(pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass) RETURNS pg_catalog.int8
 AS 'MODULE_PATHNAME','challenge_main_ingest' LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT SECURITY INVOKER;
CREATE FUNCTION @extschema@.main_identity() RETURNS pg_catalog.text
 AS 'MODULE_PATHNAME','challenge_main_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER;
""",'details':"""CREATE FUNCTION @extschema@.details_load(pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass)
      RETURNS pg_catalog.int8[] AS 'MODULE_PATHNAME','challenge_details_ingest'
      LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT SECURITY INVOKER;
CREATE FUNCTION @extschema@.details_identity() RETURNS pg_catalog.text
      AS 'MODULE_PATHNAME','challenge_details_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER;
""",'fk':"""CREATE FUNCTION @extschema@.fk_validate(pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass)
      RETURNS pg_catalog.int8[] AS 'MODULE_PATHNAME','challenge_fk_validate'
      LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT SECURITY INVOKER;
CREATE FUNCTION @extschema@.fk_identity() RETURNS pg_catalog.text
      AS 'MODULE_PATHNAME','challenge_fk_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER;
"""}


def stage(kind,body):
    build=json.loads((ROOT/('reports/native-turbo-'+kind+'-build.json')).read_text());assert build['returncode']==0
    source=(ROOT/build['source']).resolve();library=(ROOT/build['library']).resolve()
    assert source.is_relative_to(ROOT/'native') and library.is_relative_to(ROOT/'.runtime/native_build')
    sha=hashlib.sha256(source.read_bytes()).hexdigest();assert sha==build['source_sha256']
    assert hashlib.sha256(library.read_bytes()).hexdigest()==build['library_sha256']
    package=ROOT/'.runtime/postgresql/pgsql';extension='challenge_'+kind+'_'+sha[:16]
    control="comment = 'Guarded native turbo "+kind+" research, SECURITY INVOKER'\ndefault_version = '0.1'\nmodule_pathname = '$libdir/"+library.stem+"'\nrelocatable = false\nsuperuser = true\ntrusted = true\n"
    records=[]
    for path,data in [(package/'lib'/library.name,library.read_bytes()),(package/'share/extension'/(extension+'.control'),control.encode()),(package/'share/extension'/(extension+'--0.1.sql'),body.encode())]:
        assert path.resolve().is_relative_to(package.resolve())
        if path.exists():assert path.read_bytes()==data,'Never replace different installed native software'
        else:
            with path.open('xb') as writer:writer.write(data);writer.flush();os.fsync(writer.fileno())
        records.append({'path':str(path.relative_to(ROOT)),'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)})
    vendor=json.loads((ROOT/'vendor/postgresql-17.11/SOURCE.json').read_text())
    for item in vendor['files']:assert hashlib.sha256((ROOT/item['path']).read_bytes()).hexdigest()==item['sha256']
    write_report(ROOT/('reports/native-turbo-'+kind+'-trusted-package.json'),{'kind':'private_native_turbo_'+kind+'_package','extension':extension,'version':'0.1','source':str(source.relative_to(ROOT)),'source_sha256':sha,'build_receipt':build['retained_build_receipt'],'files':records,'vendor_sources':vendor,'database_objects_created':False,'application_integration':False,'official_eligibility_unproven':True})
    print('Pinned package staged:',extension,flush=True)


def main():
    for kind,body in BODIES.items():stage(kind,body)


if __name__=='__main__':main()
