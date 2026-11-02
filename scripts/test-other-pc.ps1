param(
    [ValidateSet(64,100000,1000000)][int]$Rows=100000,
    [ValidateRange(1,20)][int]$Repeats=3,
    [ValidateSet('turbo','shared')][string]$Candidate='turbo',
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
if (-not (Test-Path 'PORTABLE_MANIFEST.json')) {
    throw 'Extract dist/pipeline-test-windows-turbo.zip into a fresh folder, then run this script there. A Git clone alone does not include the unpacked PostgreSQL runtime.'
}
if (Test-Path '.runtime/native_pgdata') {
    throw 'This folder already contains test data. Extract the ZIP into a fresh folder for a clean run; existing data is preserved.'
}
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
$taskOutput="reports/portable-$Candidate-$Rows-$taskStamp.json"
Invoke-TestCommand -Executable $taskPython -CommandArguments @('scripts/start_portable_lab.py')
$taskUseMinimal=$MinimalWal.IsPresent
try {
    if ($taskUseMinimal) {
        Invoke-TestCommand -Executable $taskPython -CommandArguments @('-m','scripts.native_wal_mode','--mode','minimal','--output',"reports/portable-wal-minimal-$taskStamp.json")
    }
    Invoke-TestCommand -Executable $taskPython -CommandArguments @('scripts/bootstrap_native_control.py')
    Invoke-TestCommand -Executable $taskPython -CommandArguments @('-u','-m','bench.portable_pair','--candidate',$Candidate,'--rows',"$Rows",'--repeats',"$Repeats",'--output',$taskOutput)
} finally {
    if ($taskUseMinimal) {
        Invoke-TestCommand -Executable $taskPython -CommandArguments @('-m','scripts.native_wal_mode','--mode','replica','--output',"reports/portable-wal-restored-$taskStamp.json")
    }
}
Write-Host "Completed. Result: $taskOutput"
Write-Host "Compare the official and $Candidate times from this PC; this is a local benchmark, not a Kaggle score."
