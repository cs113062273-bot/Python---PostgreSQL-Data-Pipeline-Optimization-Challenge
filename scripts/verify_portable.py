"""Verify every shipped byte before installing or executing the portable bundle."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    manifest = json.loads((ROOT / 'PORTABLE_MANIFEST.json').read_text(encoding='utf8'))
    for item in manifest['files']:
        path = (ROOT / item['path']).resolve()
        if not path.is_relative_to(ROOT) or not path.is_file():
            raise RuntimeError('Missing or unsafe bundle path: ' + item['path'])
        if hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']:
            raise RuntimeError('Bundle hash mismatch: ' + item['path'])
    print('Portable bundle:', len(manifest['files']), 'file hashes PASS; no prior database included')


if __name__ == '__main__':
    main()
