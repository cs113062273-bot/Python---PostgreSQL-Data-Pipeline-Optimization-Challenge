"""Verify/stage exactly four native packages, or rebuild them from pinned C."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from bench.common import write_report


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build',action='store_true')
    parser.add_argument('--compiler',help='Zig 0.14.1 executable for Windows source builds')
    parser.add_argument('--sdk',type=Path,help='Full PostgreSQL 17.11 pgsql directory (include/server and lib/postgres.lib)')
    args=parser.parse_args()
    spec=json.loads((ROOT/'native/packages.json').read_text(encoding='utf8'))
    target=ROOT/'.runtime/postgresql/pgsql'
    assert target.is_dir(),'Install the pinned PostgreSQL runtime first'
    if args.build:
        assert sys.platform=='win32','This release is qualified only on Windows x64'
        assert args.compiler and args.sdk,'Source builds require --compiler and --sdk'
        version=subprocess.check_output([args.compiler,'version'],text=True).strip()
        assert version=='0.14.1',version
        assert (args.sdk/'include/server/postgres.h').is_file()
        pg_version=(args.sdk/'include/server/pg_config.h').read_text()
        assert '#define PG_VERSION "17.11"' in pg_version,'Matching PostgreSQL 17.11 SDK required'
    for kind,entry in spec['packages'].items():
        info=copy.deepcopy(entry['metadata'])
        sources=info.get('source_files',[{'path':info.get('source'),'sha256':info['source_sha256']}])
        for item in sources+info['vendor_sources']['files']:
            path=(ROOT/item['path']).resolve()
            assert path.is_relative_to(ROOT) and sha(path)==item['sha256'],item['path']
        if 'configuration' in info:
            identity={'source_files':info['source_files'],'configuration':info['configuration']}
            assert hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest()==info['source_sha256']
        library_item=next(item for item in info['files'] if item['path'].endswith('.dll'))
        library=ROOT/library_item['path']
        library.parent.mkdir(parents=True,exist_ok=True)
        if args.build:
            assert not library.exists(),'Never replace a possibly loaded DLL; build in a fresh extraction'
            compile_sources=sources[:2] if kind=='main' else sources[:1]
            argv=[args.compiler,'cc','-target','x86_64-windows-gnu','-std=c11','-O2','-g',
                  '-fno-strict-aliasing','-fwrapv','-shared','-DWIN32','-DPG_C_PRINTF_ATTRIBUTE=printf',
                  '-DCHALLENGE_PROBE_SOURCE_SHA="'+info['source_sha256']+'"']
            if kind=='main':
                argv+=['-DCHALLENGE_COPY_BATCH_TUPLES='+str(info['configuration']['batch_rows']),
                       '-DCHALLENGE_COPY_BATCH_BYTES='+str(info['configuration']['batch_bytes'])]
            include=args.sdk/'include'
            argv+=['-I'+str(include/'server/port/win32'),'-I'+str(include/'server'),'-I'+str(include)]
            argv += [str(ROOT/item['path']) for item in compile_sources]
            argv += [str(args.sdk/'lib/postgres.lib'),'-o',str(library)]
            cache=ROOT/'.runtime/compiler-cache'
            env={**os.environ,'ZIG_GLOBAL_CACHE_DIR':str(cache/'global'),'ZIG_LOCAL_CACHE_DIR':str(cache/'local')}
            subprocess.run(argv,cwd=ROOT,env=env,check=True)
            library_item['sha256']=sha(library)
            library_item['bytes']=library.stat().st_size
        else:
            assert library.is_file() and sha(library)==library_item['sha256'],str(library)
        for name in entry['extension_sources']:
            source=ROOT/name
            item=next(item for item in info['files'] if Path(item['path']).name==source.name)
            assert sha(source)==item['sha256']
            destination=(ROOT/item['path']).resolve()
            assert destination.is_relative_to(target.resolve())
            destination.parent.mkdir(parents=True,exist_ok=True)
            if destination.exists():assert destination.read_bytes()==source.read_bytes()
            else:
                with destination.open('xb') as writer:
                    writer.write(source.read_bytes());writer.flush();os.fsync(writer.fileno())
        write_report(ROOT/entry['receipt'],info)
        print(kind,info['source_sha256'],'source / DLL / SQL hashes PASS',flush=True)


if __name__=='__main__':main()
