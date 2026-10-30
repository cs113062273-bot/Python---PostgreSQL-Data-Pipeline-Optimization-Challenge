param(
    [ValidateSet(64,100000,1000000)][int]$Rows=100000,
    [ValidateRange(1,20)][int]$Repeats=3,
    [string]$PythonExe='python',
    [switch]$MinimalWal
)
$ErrorActionPreference='Stop'
$taskRoot=Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $taskRoot
function Invoke-TestCommand {
    param([string]$Executable,[string[]]$CommandArguments)
    & $Executable @CommandArguments
    if ($LASTEXITCODE -ne 0) { throw "Command failed: $Executable $CommandArguments" }
}
Get-Command git -ErrorAction Stop | Out-Null
if (-not (Test-Path '.venv/Scripts/python.exe')) {
    Invoke-TestCommand -Executable $PythonExe -CommandArguments @('-m','venv','.venv')
}
$taskPython=Join-Path $taskRoot '.venv/Scripts/python.exe'
Invoke-TestCommand -Executable $taskPython -CommandArguments @('scripts/verify_portable.py')
Invoke-TestCommand -Executable $taskPython -CommandArguments @('-m','pip','install','-r','requirements-lock.txt')
# The benchmark records Git metadata. An empty local marker commit supplies
# that metadata; PORTABLE_MANIFEST.json is the actual source/asset identity.
if (-not (Test-Path '.git')) {
    Invoke-TestCommand -Executable 'git' -CommandArguments @('init','--quiet')
    Invoke-TestCommand -Executable 'git' -CommandArguments @('-c','user.name=Portable benchmark','-c','user.email=portable@localhost','commit','--allow-empty','--quiet','-m','Portable test workspace; source identity in PORTABLE_MANIFEST.json')
}
Invoke-TestCommand -Executable $taskPython -CommandArguments @('scripts/verify_upstream.py')
$taskStamp=Get-Date -Format 'yyyyMMdd-HHmmss'
$taskOutput="reports/portable-$Rows-$taskStamp.json"
if (Test-Path '.runtime/native_pgdata/PG_VERSION') {
    throw 'This folder already contains a test database. Extract the ZIP into a fresh folder for a clean run; existing data is preserved.'
}
Invoke-TestCommand -Executable $taskPython -CommandArguments @('scripts/start_portable_lab.py')
$taskUseMinimal=$MinimalWal.IsPresent
try {
    if ($taskUseMinimal) {
        Invoke-TestCommand -Executable $taskPython -CommandArguments @('-m','scripts.native_wal_mode','--mode','minimal','--output',"reports/portable-wal-minimal-$taskStamp.json")
    }
    Invoke-TestCommand -Executable $taskPython -CommandArguments @('scripts/bootstrap_native_control.py')
    Invoke-TestCommand -Executable $taskPython -CommandArguments @('-m','bench.portable_pair','--rows',"$Rows",'--repeats',"$Repeats",'--output',$taskOutput)
} finally {
    if ($taskUseMinimal) {
        Invoke-TestCommand -Executable $taskPython -CommandArguments @('-m','scripts.native_wal_mode','--mode','replica','--output',"reports/portable-wal-restored-$taskStamp.json")
    }
}
Write-Host "Completed. Result: $taskOutput"
Write-Host 'Compare the official and shared times from this PC; this is a local benchmark, not a Kaggle score.'
