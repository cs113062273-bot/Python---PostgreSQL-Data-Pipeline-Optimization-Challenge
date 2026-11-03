param(
    [ValidateSet(64,100000,1000000)][int]$Rows=64,
    [ValidateRange(1,20)][int]$Repeats=1,
    [string]$PythonExe='python',
    [string]$PostgreSQLArchive,
    [switch]$SkipSetup
)
$ErrorActionPreference='Stop'
$taskRoot=Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $taskRoot
if (Test-Path '.runtime/native_pgdata') {
    throw 'A database directory already exists. Use a fresh extraction; existing data is preserved.'
}
if (-not $SkipSetup) {
    & "$PSScriptRoot/setup.ps1" -PythonExe $PythonExe -PostgreSQLArchive $PostgreSQLArchive
}
$taskPython=Join-Path $taskRoot '.venv/Scripts/python.exe'
function Invoke-Reproduction {
    param([string[]]$Arguments)
    & $taskPython -c 'import subprocess,sys; sys.exit(subprocess.call(sys.argv[1:],stderr=subprocess.STDOUT))' $taskPython @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Reproduction failed: $Arguments" }
}
Get-Command git -ErrorAction Stop | Out-Null
if (-not (Test-Path '.git')) {
    & git init --quiet
    if ($LASTEXITCODE -ne 0) { throw 'Cannot initialize extracted source metadata' }
    & git add --all
    if ($LASTEXITCODE -ne 0) { throw 'Cannot record reviewed extracted source files' }
    & git -c 'user.name=Portable reproduction' -c 'user.email=reproduction@localhost' commit --quiet -m 'Extracted release; authoritative source commit is in PORTABLE_MANIFEST.json'
    if ($LASTEXITCODE -ne 0) { throw 'Cannot record extracted source metadata' }
}
$env:PGHOST='127.0.0.1'
$env:PGPORT='55433'
$env:PGUSER='native_probe'
$env:PGCONNECT_TIMEOUT='30'
$env:CHALLENGE_DSN='host=127.0.0.1 port=55433 user=native_probe'
$env:CHALLENGE_PSQL=Join-Path $taskRoot '.runtime/postgresql/pgsql/bin/psql.exe'
Invoke-Reproduction -Arguments @('-m','pytest','tests/test_recipes.py','tests/test_report_receipts.py','-q')
$taskStamp=Get-Date -Format 'yyyyMMdd-HHmmss'
$taskOutput="reports/reproduction-$Rows-$taskStamp.json"
$taskStarted=$false
try {
    Invoke-Reproduction -Arguments @('scripts/start_portable_lab.py')
    $taskStarted=$true
    Invoke-Reproduction -Arguments @('-m','scripts.native_wal_mode','--mode','minimal','--output',"reports/wal-mode-$taskStamp.json")
    Invoke-Reproduction -Arguments @('scripts/bootstrap_native_control.py')
    Invoke-Reproduction -Arguments @('-u','-m','bench.portable_pair','--candidate','temp-stage','--rows',"$Rows",'--repeats',"$Repeats",'--output',$taskOutput)
} finally {
    if ($taskStarted) {
        & (Join-Path $taskRoot '.runtime/postgresql/pgsql/bin/pg_ctl.exe') -D (Join-Path $taskRoot '.runtime/native_pgdata') -m fast -w stop
        if ($LASTEXITCODE -ne 0) { Write-Warning 'The isolated test cluster did not stop; inspect its local log.' }
    }
}
Write-Host "Paired original/temp-stage reproduction complete: $taskOutput"
Write-Host 'Local Windows benchmark only. Exact outputs, native integrity checks, durable commit and six versions are verified.'
