"""Package only reviewed source plus four pinned native DLLs; no databases."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import zipfile

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native-runtime',type=Path,required=True,help='Verified pgsql directory containing the four pinned DLLs')
    parser.add_argument('--output',default='dist/pipeline-temp-stage-windows-x64.zip')
    args=parser.parse_args()
    subprocess.run([__import__('sys').executable,'scripts/audit_release.py'],cwd=ROOT,check=True)
    output=(ROOT/args.output).resolve()
    assert output.is_relative_to(ROOT/'dist') and not output.exists()
    paths=json.loads((ROOT/'SOURCE_FILES.json').read_text())['files']
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True).strip(),'Commit all reviewed source changes before packaging'
    # Read committed blobs, not worktree bytes: Windows line-ending conversion
    # must not make the ZIP differ from its authoritative Git commit.
    source_archive=zipfile.ZipFile(io.BytesIO(subprocess.check_output(['git','archive','--format=zip','HEAD'],cwd=ROOT)))
    assert {name for name in source_archive.namelist() if not name.endswith('/')}==set(paths)
    files={name:source_archive.read(name) for name in paths}
    spec=json.loads((ROOT/'native/packages.json').read_text())
    for entry in spec['packages'].values():
        item=next(item for item in entry['metadata']['files'] if item['path'].endswith('.dll'))
        path=args.native_runtime/'lib'/Path(item['path']).name
        assert path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest()==item['sha256'],path
        files[Path(item['path']).as_posix()]=path.read_bytes()
    records=[{'path':name,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()} for name,data in sorted(files.items())]
    manifest={'kind':'reviewed_temp_stage_source_and_native_dlls','source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
              'databases_included':False,'python_environment_included':False,'postgresql_download_required':True,
              'scope':'Windows x64 local reproduction, not official Linux/SSH qualification','files':records}
    output.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(output,'x',compression=zipfile.ZIP_DEFLATED,compresslevel=9) as archive:
        for name,data in sorted(files.items()):archive.writestr(name,data)
        archive.writestr('PORTABLE_MANIFEST.json',json.dumps(manifest,indent=2)+'\n')
    with zipfile.ZipFile(output) as archive:
        assert archive.testzip() is None
        for item in records:assert hashlib.sha256(archive.read(item['path'])).hexdigest()==item['sha256']
    digest=hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_suffix(output.suffix+'.sha256').write_text(digest+'  '+output.name+'\n',encoding='ascii')
    print(output.name,output.stat().st_size,'bytes;',len(records),'verified files;',digest)


if __name__=='__main__':main()
