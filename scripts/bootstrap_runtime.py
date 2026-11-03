"""Download the fixed official Windows PostgreSQL distribution and check SHA-256."""
import argparse
import hashlib
import json
from pathlib import Path,PurePosixPath
import shutil
import sys
import urllib.request
import zipfile

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',type=Path,help='Reuse a local copy of the exact pinned official archive')
    args=parser.parse_args()
    assert sys.platform=='win32','The delivered native runtime requires Windows x64'
    spec=json.loads((ROOT/'native/runtime.json').read_text())
    target=ROOT/'.runtime/postgresql'
    executable=target/'pgsql/bin/postgres.exe'
    if executable.exists():
        actual=hashlib.sha256(executable.read_bytes()).hexdigest()
        assert actual==spec['postgres_exe_sha256'],'Unexpected existing PostgreSQL runtime'
        print('Pinned PostgreSQL runtime already installed')
        return
    archive=args.archive or ROOT/'.runtime/downloads/postgresql.zip'
    if not archive.exists():
        archive.parent.mkdir(parents=True,exist_ok=True)
        partial=archive.with_suffix('.part')
        assert not partial.exists(),'A previous partial download is preserved; inspect it before retry'
        print('Downloading official PostgreSQL 17.11 distribution',flush=True)
        with urllib.request.urlopen(spec['url'],timeout=60) as response,partial.open('xb') as writer:
            shutil.copyfileobj(response,writer)
        partial.replace(archive)
    digest=hashlib.sha256()
    with archive.open('rb') as stream:
        for block in iter(lambda:stream.read(4*1024*1024),b''):digest.update(block)
    assert archive.stat().st_size==spec['bytes'] and digest.hexdigest()==spec['sha256'],'PostgreSQL archive identity mismatch'
    with zipfile.ZipFile(archive) as packed:
        for member in packed.infolist():
            path=PurePosixPath(member.filename)
            assert not path.is_absolute() and '..' not in path.parts,'Unsafe archive path'
            if member.is_dir() or not member.filename.startswith(('pgsql/bin/','pgsql/lib/','pgsql/share/','pgsql/include/')):continue
            destination=(target/Path(*path.parts)).resolve()
            assert destination.is_relative_to(target.resolve())
            destination.parent.mkdir(parents=True,exist_ok=True)
            with packed.open(member) as source:
                if destination.exists():assert hashlib.sha256(source.read()).digest()==hashlib.sha256(destination.read_bytes()).digest()
                else:
                    with destination.open('xb') as writer:shutil.copyfileobj(source,writer)
    assert hashlib.sha256(executable.read_bytes()).hexdigest()==spec['postgres_exe_sha256']
    print('Official PostgreSQL archive hash and executable PASS',flush=True)


if __name__=='__main__':main()
