"""Build an isolated PG17 research DLL using the verified portable Zig SDK."""
import hashlib
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from bench.common import write_report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',default='native/challenge_probe.c')
    parser.add_argument('--output',default='reports/native-c-probe-build.json')
    args=parser.parse_args()
    source=(ROOT/args.source).resolve();assert source.is_relative_to(ROOT/'native') and source.suffix=='.c'
    data=source.read_bytes();sha=hashlib.sha256(data).hexdigest()
    compiler=ROOT/'.runtime/toolchains/zig-x86_64-windows-0.14.1/zig.exe'
    if not compiler.exists():compiler=ROOT/'.runtime/toolchains/mirror/zig-x86_64-windows-0.14.1/zig.exe'
    include=ROOT/'.runtime/postgresql/pgsql/include'
    if not compiler.exists() or not (include/'server/postgres.h').exists():raise RuntimeError('Portable compiler and matching official PG17 server SDK are required')
    driver_sha=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    directory=ROOT/'.runtime/native_build'/(sha[:16]+'-'+driver_sha[:16]);directory.mkdir(parents=True,exist_ok=True)
    library=directory/('challenge_probe_'+sha[:16]+'.dll')
    if library.exists():raise RuntimeError('Never overwrite a potentially loaded native library; use a new source/build identity')
    argv=[str(compiler),'cc','-target','x86_64-windows-gnu','-std=c11','-O2','-g','-fno-strict-aliasing','-fwrapv','-shared','-DWIN32','-DPG_C_PRINTF_ATTRIBUTE=printf','-I'+str(include/'server/port/win32'), '-DCHALLENGE_PROBE_SOURCE_SHA="'+sha+'"','-I'+str(include/'server'),'-I'+str(include),str(source),str(ROOT/'.runtime/postgresql/pgsql/lib/postgres.lib'),'-o',str(library)]
    cache=ROOT/'.runtime/native_zig_cache'
    env={**os.environ,'ZIG_GLOBAL_CACHE_DIR':str(cache/'global'),'ZIG_LOCAL_CACHE_DIR':str(cache/'local')}
    result=subprocess.run(argv,cwd=ROOT,capture_output=True,text=True,env=env)
    metadata={'kind':'isolated_native_probe_build','source':str(source.relative_to(ROOT)),'source_sha256':sha,'driver_sha256':driver_sha,'compiler_version':subprocess.check_output([str(compiler),'version'],text=True).strip(),'compiler_archive':json.loads((ROOT/'.runtime/toolchains/zig-0.14.1-download.json').read_text()),'command':argv,'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr,'application_integration':False}
    if result.returncode==0:
        with library.open('r+b') as artifact:artifact.flush();os.fsync(artifact.fileno())
        metadata.update({'library':str(library.relative_to(ROOT)),'library_sha256':hashlib.sha256(library.read_bytes()).hexdigest()})
    archive=ROOT/'reports/native-c-builds';archive.mkdir(exist_ok=True)
    sources=ROOT/'reports/native-source';sources.mkdir(exist_ok=True)
    source_archive=sources/(sha+'.c')
    if not source_archive.exists():source_archive.write_bytes(data)
    assert hashlib.sha256(source_archive.read_bytes()).hexdigest()==sha
    metadata['source_archive']=str(source_archive.relative_to(ROOT))
    receipt=archive/(sha[:16]+'-'+uuid.uuid4().hex[:8]+'.json');write_report(receipt,metadata)
    metadata['retained_build_receipt']=str(receipt.relative_to(ROOT))
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports');write_report(output,metadata)
    print(result.stdout);print(result.stderr,file=sys.stderr)
    if result.returncode:raise SystemExit(result.returncode)
    print('Native isolated probe built:',library,flush=True)


if __name__=='__main__':main()
