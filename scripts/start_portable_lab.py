"""Start a fresh self-contained Windows lab without depending on port 55432."""
from pathlib import Path
import socket
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bench import native_c_lab as lab


def main():
    if sys.platform != 'win32':
        raise RuntimeError('This bundled PostgreSQL runtime requires Windows x64')
    data = (ROOT / '.runtime/native_pgdata').resolve()
    binary = ROOT / '.runtime/postgresql/pgsql/bin'
    assert data.is_relative_to(ROOT / '.runtime')
    with socket.socket() as probe:
        if probe.connect_ex(('127.0.0.1', lab.PORT)) == 0:
            raise RuntimeError('Port 55433 is already in use; stop the previous lab before starting this fresh bundle')
    if data.exists():
        raise RuntimeError('Fresh extraction required; existing data directory is preserved')
    flags = subprocess.CREATE_NO_WINDOW
    initialized = subprocess.run([str(binary / 'initdb.exe'), '-D', str(data), '-U', 'native_probe', '-A', 'trust',
                    '-E', 'UTF8', '--locale=C', '--no-instructions'], capture_output=True, text=True,
                    encoding='utf8', errors='replace', creationflags=flags)
    if initialized.returncode:
        raise RuntimeError('PostgreSQL initdb failed:\n' + initialized.stdout + initialized.stderr)
    # A fresh cluster has no recovery sync backlog; allow one minute for slow
    # machines without requiring another existing cluster's locale/config.
    subprocess.run([str(binary / 'pg_ctl.exe'), 'start', '-D', str(data), '-l',
                    str(ROOT / '.runtime/portable-postgres.log'), '-o', '-h 127.0.0.1 -p 55433',
                    '-w', '-t', '60'], check=True, creationflags=flags)
    with lab.connect('postgres', autocommit=True) as c:
        settings = dict(c.execute("SELECT name,setting FROM pg_settings WHERE name IN ('fsync','full_page_writes','synchronous_commit')").fetchall())
        assert all(value == 'on' for value in settings.values()) and len(settings) == 3
    print('Fresh isolated cluster ready on 127.0.0.1:55433; all durability switches on', flush=True)


if __name__ == '__main__':
    main()
