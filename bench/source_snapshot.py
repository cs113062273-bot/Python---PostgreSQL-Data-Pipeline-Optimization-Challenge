"""Content-addressed source snapshots alongside fresh diagnostic cohorts."""
import hashlib
from bench.common import ROOT


def snapshot():
    archive=ROOT/'reports/source';archive.mkdir(exist_ok=True)
    result={}
    for directory in ('pipeline','bench','tests','scripts'):
        for path in (ROOT/directory).rglob('*.py'):
            data=path.read_text(encoding='utf-8').encode();digest=hashlib.sha256(data).hexdigest()
            target=archive/(digest+'.py')
            if not target.exists():target.write_bytes(data)
            assert hashlib.sha256(target.read_bytes()).hexdigest()==digest
            result[str(path.relative_to(ROOT))]={'sha256_normalized_lf':digest,'archive':str(target.relative_to(ROOT))}
    return result
