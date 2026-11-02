"""Package code, receipts and existing Windows runtime; never package databases."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', default='dist/pipeline-test-windows-turbo.zip')
    args = p.parse_args()
    output = (ROOT / args.output).resolve()
    assert output.is_relative_to(ROOT / 'dist') and not output.exists()
    files = {}
    for directory in ('app', 'pipeline', 'bench', 'tests', 'native', 'vendor', 'scripts', 'docs'):
        for path in (ROOT / directory).rglob('*'):
            if path.is_file() and '__pycache__' not in path.parts and path.suffix not in ('.pyc', '.log', '.tmp'):
                files[path.relative_to(ROOT).as_posix()] = path
    report_dir = ROOT / 'reports'
    for path in report_dir.glob('*trusted-package.json'):
        if path.is_file():
            files[path.relative_to(ROOT).as_posix()] = path
    # The package receipts name and hash each C extension and SQL control file.
    # Include only those recorded paths instead of the ignored 200GB lab state.
    for package_path in (ROOT / 'reports').glob('*trusted-package.json'):
        package = json.loads(package_path.read_text(encoding='utf8'))
        for item in package.get('files', []):
            path = (ROOT / item['path']).resolve()
            if not path.is_relative_to(ROOT / '.runtime/postgresql/pgsql') or not path.is_file():
                raise RuntimeError('Missing or unsafe native package file: ' + item['path'])
            files[path.relative_to(ROOT).as_posix()] = path
    postgres = ROOT / '.runtime/postgresql/pgsql'
    executable_names = ('initdb.exe', 'pg_ctl.exe', 'postgres.exe', 'psql.exe', 'pg_dump.exe', 'pg_restore.exe')
    for name in executable_names:
        path = postgres / 'bin' / name
        if not path.is_file():
            raise RuntimeError('Missing PostgreSQL runtime executable: ' + name)
        files[path.relative_to(ROOT).as_posix()] = path
    # PostgreSQL uses ICU, OpenSSL, compression, and libpq DLLs in this folder.
    # wxWidgets DLLs belong to pgAdmin and are not used by the server/CLI.
    for path in (postgres / 'bin').glob('*.dll'):
        if not path.name.lower().startswith('wx'):
            files[path.relative_to(ROOT).as_posix()] = path
    # A blank initdb loads the Snowball dictionary, PL/pgSQL and encoding
    # converters from $libdir. They are not bin DLL dependencies and a copy
    # of a previously initialized cluster can conceal their absence.
    for pattern in ('dict_snowball.dll', 'plpgsql.dll', '*_and_*.dll'):
        for path in (postgres / 'lib').glob(pattern):
            files[path.relative_to(ROOT).as_posix()] = path
    for name in ('dict_snowball.dll', 'plpgsql.dll'):
        if not (postgres / 'lib' / name).is_file():
            raise RuntimeError('Missing PostgreSQL initialization library: ' + name)
    for path in (postgres / 'share').rglob('*'):
        if path.is_file():
            files[path.relative_to(ROOT).as_posix()] = path
    for name in ('README.md', 'PORTABLE_TEST.md', 'LICENSE', 'launch.py', 'requirements-lock.txt', 'upstream.json', '.gitignore', '.gitattributes'):
        files[name] = ROOT / name
    for name in files:
        if name.startswith('.runtime/') and not name.startswith('.runtime/postgresql/pgsql/'):
            raise RuntimeError('Only PostgreSQL runtime may be packaged: ' + name)
    records = [{'path': name, 'bytes': path.stat().st_size, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()} for name, path in sorted(files.items())]
    manifest = {'kind': 'windows_x64_portable_benchmark_source_and_runtime',
                'timestamp_utc': datetime.now(timezone.utc).isoformat(),
                'source_base_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                'working_tree_files_included': True, 'databases_included': False,
                'source_identity': 'Individual byte hashes below, including uncommitted latest candidate; base commit alone is not bundle identity.',
                'python_environment_included': False, 'requirements_install_needs_network': True,
                'linux_supported': False, 'files': records}
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, 'x', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for name, path in sorted(files.items()):
            archive.write(path, name)
        archive.writestr('PORTABLE_MANIFEST.json', json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
    with zipfile.ZipFile(output) as archive:
        assert archive.testzip() is None
        for item in records:
            assert hashlib.sha256(archive.read(item['path'])).hexdigest() == item['sha256']
    receipt = {'archive': str(output.relative_to(ROOT)), 'bytes': output.stat().st_size,
               'sha256': hashlib.sha256(output.read_bytes()).hexdigest(), 'files': len(records),
               'uncompressed_bytes': sum(r['bytes'] for r in records), 'all_zip_entries_hash_verified': True,
               'databases_included': False}
    output.with_suffix('.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf8')
    print(json.dumps(receipt, indent=2))


if __name__ == '__main__':
    main()
