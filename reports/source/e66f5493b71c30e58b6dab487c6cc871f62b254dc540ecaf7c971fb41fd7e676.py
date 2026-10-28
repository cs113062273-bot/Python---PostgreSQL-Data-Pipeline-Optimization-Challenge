"""Stage an opt-in, source-pinned shared-key FK validation research package."""
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from bench.common import write_report


def main():
    build=json.loads((ROOT/'reports/native-fk-validate-build-v2.json').read_text())
    source=ROOT/'native/challenge_fk_validate.c';sha=hashlib.sha256(source.read_bytes()).hexdigest()
    assert build['returncode']==0 and build['source_sha256']==sha
    library=(ROOT/build['library']).resolve();assert library.is_relative_to(ROOT/'.runtime/native_build')
    assert hashlib.sha256(library.read_bytes()).hexdigest()==build['library_sha256']
    package=ROOT/'.runtime/postgresql/pgsql';extension='challenge_fk_'+sha[:16]
    control="comment = 'Shared actual-key FK validation research, SECURITY INVOKER'\ndefault_version = '0.1'\nmodule_pathname = '$libdir/"+library.stem+"'\nrelocatable = false\nsuperuser = true\ntrusted = true\n"
    body="""CREATE FUNCTION @extschema@.fk_validate(pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass)
      RETURNS pg_catalog.int8[] AS 'MODULE_PATHNAME','challenge_fk_validate'
      LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT SECURITY INVOKER;
CREATE FUNCTION @extschema@.fk_identity() RETURNS pg_catalog.text
      AS 'MODULE_PATHNAME','challenge_fk_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER;
"""
    records=[]
    for path,data in [(package/'lib'/library.name,library.read_bytes()),(package/'share/extension'/(extension+'.control'),control.encode()),(package/'share/extension'/(extension+'--0.1.sql'),body.encode())]:
        assert path.resolve().is_relative_to(package.resolve())
        if path.exists():assert path.read_bytes()==data,'Never replace installed native software'
        else:
            with path.open('xb') as writer:writer.write(data);writer.flush();os.fsync(writer.fileno())
        records.append({'path':str(path.relative_to(ROOT)),'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)})
    output=ROOT/'reports/native-fk-trusted-package.json';assert not output.exists()
    write_report(output,{'kind':'private_native_full_fk_validation_package','extension':extension,'version':'0.1','source':str(source.relative_to(ROOT)),'source_sha256':sha,'build_receipt':build['retained_build_receipt'],'files':records,'database_objects_created':False,'application_integration':False,'official_eligibility_unproven':True})
    print('Pinned FK validation software staged:',extension,flush=True)


if __name__=='__main__':main()
