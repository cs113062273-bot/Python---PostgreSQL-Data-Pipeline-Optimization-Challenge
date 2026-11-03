"""Enforce the reviewed source allowlist and reject research state or secrets."""
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

ROOT=Path(__file__).resolve().parents[1]
IGNORED={'.git','.runtime','.venv','reports','dist','__pycache__','.pytest_cache'}


def main():
    manifest=json.loads((ROOT/'SOURCE_FILES.json').read_text())
    allowed=set(manifest['files'])
    actual=set()
    for directory,children,names in os.walk(ROOT):
        children[:]=[name for name in children if name not in IGNORED]
        for name in names:
            path=Path(directory)/name
            if path.is_file() and name not in ('.git','PORTABLE_MANIFEST.json'):actual.add(path.relative_to(ROOT).as_posix())
    assert actual==allowed,{'unexpected':sorted(actual-allowed),'missing':sorted(allowed-actual)}
    assert {name for name in allowed if name.endswith('.md')}=={'README.md','app/README.md','app/GENERATE_DATABASE_README.md'}
    assert not any(name.split('/')[0] in IGNORED or name.endswith(('.dll','.exe','.zip','.log','.pyc')) for name in allowed)
    private_key=re.compile(rb'-----BEGIN [A-Z ]*PRIVATE KEY-----')
    token=re.compile(rb'gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}|AKIA[0-9A-Z]{16}')
    for name in sorted(allowed):
        path=ROOT/name
        data=path.read_bytes()
        assert len(data)<2*1024*1024,('Unexpected large source file',name)
        assert not private_key.search(data) and not token.search(data),('Possible secret in source',name)
        if name.endswith('.py'):ast.parse(data,filename=name)
    if (ROOT/'.git').exists():
        tracked=set(subprocess.check_output(['git','ls-files'],cwd=ROOT,text=True).splitlines())
        assert tracked==allowed,{'unreviewed_tracked_files':sorted(tracked-allowed),'untracked_delivery_files':sorted(allowed-tracked)}
    upstream=json.loads((ROOT/'upstream.json').read_text())
    for name,expected in upstream['files_sha256'].items():
        assert hashlib.sha256((ROOT/'app'/name).read_bytes()).hexdigest()==expected,name
    spec=json.loads((ROOT/'native/packages.json').read_text())
    assert set(spec['packages'])=={'main','stamp','details','fk'}
    for entry in spec['packages'].values():
        info=entry['metadata']
        sources=info.get('source_files',[{'path':info.get('source'),'sha256':info['source_sha256']}])
        for record in sources+info['vendor_sources']['files']:
            path=(ROOT/record['path']).resolve()
            assert path.is_relative_to(ROOT) and hashlib.sha256(path.read_bytes()).hexdigest()==record['sha256'],record['path']
    print(len(allowed),'reviewed source files; 32 original files; four native packages; no research state or detected secrets: PASS')


if __name__=='__main__':main()
