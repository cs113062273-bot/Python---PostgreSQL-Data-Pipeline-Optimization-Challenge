param([string]$PythonExe='python',[int]$Port=55432)
$ErrorActionPreference='Stop'
$taskRoot=Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $taskRoot
New-Item -ItemType Directory -Force .runtime | Out-Null
if (-not (Test-Path .venv/Scripts/python.exe)) {
    & $PythonExe -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw 'Python 3.10+ is required; supply -PythonExe with its executable path' }
}
& .venv/Scripts/python.exe -m pip install -r requirements-lock.txt
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed' }
$taskArchive=Join-Path $taskRoot '.runtime/postgresql.zip'
if (-not (Test-Path -LiteralPath $taskArchive)) {
    curl.exe -L --fail --output $taskArchive https://get.enterprisedb.com/postgresql/postgresql-17.11-3-windows-x64-binaries.zip
    if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL binary download failed' }
}
@'
from zipfile import ZipFile
from pathlib import Path
with ZipFile('.runtime/postgresql.zip') as archive:
    for name in archive.namelist():
        if name.startswith(('pgsql/bin/','pgsql/lib/','pgsql/share/')):
            archive.extract(name,'.runtime/postgresql')
'@ | & .venv/Scripts/python.exe -
if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL extraction failed' }
$taskBin=Join-Path $taskRoot '.runtime/postgresql/pgsql/bin'
if (-not (Test-Path .runtime/pgdata/PG_VERSION)) {
    & "$taskBin/initdb.exe" -D .runtime/pgdata -U challenge --auth-local=trust --auth-host=trust --encoding=UTF8 --locale=C
    if ($LASTEXITCODE -ne 0) { throw 'initdb failed' }
}
& "$taskBin/pg_ctl.exe" -D .runtime/pgdata status
if ($LASTEXITCODE -ne 0) {
    & "$taskBin/pg_ctl.exe" -D .runtime/pgdata -l .runtime/postgres.log -o "-h 127.0.0.1 -p $Port" -w start
    if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL start failed' }
}
Write-Host "Local disposable cluster ready at 127.0.0.1:$Port, role challenge. No system service was installed."
Write-Host 'Set PGHOST=127.0.0.1, PGPORT to the selected port, PGUSER=challenge, and CHALLENGE_PSQL to the extracted psql.exe.'
