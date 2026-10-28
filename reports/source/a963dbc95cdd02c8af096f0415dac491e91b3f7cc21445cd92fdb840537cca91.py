"""Stage an audited, pure-argument trusted extension in the private PG package.

This installs software files only. It creates no extension/functions in any
database. Database-owner installation and transactional removal are tested in
the isolated research cluster before considering application integration.
"""
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from bench.common import write_report


def main():
    build=json.loads((ROOT/'reports/native-c-probe-build.json').read_text());assert build['returncode']==0
    sha=build['source_sha256'];assert hashlib.sha256((ROOT/'native/challenge_probe.c').read_bytes()).hexdigest()==sha
    library=(ROOT/build['library']).resolve();assert hashlib.sha256(library.read_bytes()).hexdigest()==build['library_sha256']
    package=ROOT/'.runtime/postgresql/pgsql';extension='challenge_native_'+sha[:16]+'_v2'
    control="comment = 'Pure argument-only JSONB normalizer research'\ndefault_version = '0.2'\nmodule_pathname = '$libdir/"+library.stem+"'\nrelocatable = false\nsuperuser = true\ntrusted = true\n"
    sql="""CREATE FUNCTION @extschema@.normalize(pg_catalog.jsonb) RETURNS pg_catalog.record
      AS 'MODULE_PATHNAME','challenge_compact12' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER CALLED ON NULL INPUT;
CREATE FUNCTION @extschema@.axes(pg_catalog.bytea)
      RETURNS TABLE(position_number pg_catalog.int2,weight pg_catalog.int4,axis_kind pg_catalog.text,gap pg_catalog.int4)
      AS 'MODULE_PATHNAME','challenge_axis_rows' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER ROWS 14;
CREATE FUNCTION @extschema@.identity() RETURNS pg_catalog.text
      AS 'MODULE_PATHNAME','challenge_probe_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER;
"""
    files=[(package/'lib'/library.name,library.read_bytes()),(package/'share/extension'/(extension+'.control'),control.encode()),(package/'share/extension'/(extension+'--0.2.sql'),sql.encode())]
    records=[]
    for path,data in files:
        assert path.resolve().is_relative_to(package.resolve())
        if path.exists():assert path.read_bytes()==data,'Never replace a different deployed native build'
        else:
            with path.open('xb') as writer:writer.write(data);writer.flush();os.fsync(writer.fileno())
        records.append({'path':str(path.relative_to(ROOT)),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'bytes':len(data)})
    write_report(ROOT/'reports/native-c-trusted-package-v2.json',{'kind':'private_native_software_package','extension':extension,'version':'0.2','source_sha256':sha,'build_receipt':build['retained_build_receipt'],'files':records,'database_objects_created':False,'application_integration':False,'eligibility_unproven':True})
    print('Private native extension package staged:',extension,flush=True)


if __name__=='__main__':main()
