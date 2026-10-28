"""Stage a separately guarded SECURITY INVOKER typed-COPY trusted package."""
import hashlib
import argparse
import json
import os
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from bench.common import write_report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-report',default='reports/native-details-ingest-build.json')
    parser.add_argument('--output',default='reports/native-details-trusted-package.json')
    args=parser.parse_args()
    receipt=(ROOT/args.build_report).resolve();assert receipt.is_relative_to(ROOT/'reports')
    build=json.loads(receipt.read_text());assert build['returncode']==0
    source=(ROOT/build['source']).resolve();assert source.is_relative_to(ROOT/'native') and source.suffix=='.c'
    sha=build['source_sha256'];assert hashlib.sha256(source.read_bytes()).hexdigest()==sha
    library=(ROOT/build['library']).resolve();assert hashlib.sha256(library.read_bytes()).hexdigest()==build['library_sha256']
    vendor=json.loads((ROOT/'vendor/postgresql-17.11/SOURCE.json').read_text())
    for item in vendor['files']:assert hashlib.sha256((ROOT/item['path']).read_bytes()).hexdigest()==item['sha256']
    package=ROOT/'.runtime/postgresql/pgsql';extension='challenge_details_'+sha[:16]
    control="comment = 'Guarded typed COPY details research, SECURITY INVOKER'\ndefault_version = '0.1'\nmodule_pathname = '$libdir/"+library.stem+"'\nrelocatable = false\nsuperuser = true\ntrusted = true\n"
    body="""CREATE FUNCTION @extschema@.details_load(pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass)
      RETURNS pg_catalog.int8[] AS 'MODULE_PATHNAME','challenge_details_ingest'
      LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT SECURITY INVOKER;
CREATE FUNCTION @extschema@.details_identity() RETURNS pg_catalog.text
      AS 'MODULE_PATHNAME','challenge_details_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER;
"""
    files=[(package/'lib'/library.name,library.read_bytes()),(package/'share/extension'/(extension+'.control'),control.encode()),(package/'share/extension'/(extension+'--0.1.sql'),body.encode())]
    records=[]
    for path,data in files:
        assert path.resolve().is_relative_to(package.resolve())
        if path.exists():assert path.read_bytes()==data,'Never replace different installed native software'
        else:
            with path.open('xb') as writer:writer.write(data);writer.flush();os.fsync(writer.fileno())
        records.append({'path':str(path.relative_to(ROOT)),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'bytes':len(data)})
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports')
    write_report(output,{'kind':'private_native_details_software_package','extension':extension,'version':'0.1','source':str(source.relative_to(ROOT)),'source_sha256':sha,'build_receipt':build['retained_build_receipt'],'files':records,'vendor_sources':vendor,'database_objects_created':False,'application_integration':False,'official_eligibility_unproven':True})
    print('Private typed-COPY package staged:',extension,flush=True)


if __name__=='__main__':main()
