param(
    [string]$PythonExe='python',
    [string]$PostgreSQLArchive,
    [switch]$BuildNative,
    [string]$Compiler,
    [string]$Sdk
)
$ErrorActionPreference='Stop'
$taskRoot=Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $taskRoot
function Invoke-Checked {
    param([string]$Executable,[string[]]$Arguments)
    & $PythonExe -c 'import subprocess,sys; sys.exit(subprocess.call(sys.argv[1:],stderr=subprocess.STDOUT))' $Executable @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Command failed: $Executable $Arguments" }
}
Invoke-Checked -Executable $PythonExe -Arguments @('-c','import sys,struct; assert sys.version_info[:2]==(3,11) and struct.calcsize(chr(80))==8')
if (Test-Path 'PORTABLE_MANIFEST.json') {
    Invoke-Checked -Executable $PythonExe -Arguments @('scripts/verify_portable.py')
}
if (-not (Test-Path '.venv/Scripts/python.exe')) {
    Invoke-Checked -Executable $PythonExe -Arguments @('-m','venv','.venv')
}
$taskPython=Join-Path $taskRoot '.venv/Scripts/python.exe'
Invoke-Checked -Executable $taskPython -Arguments @('-m','pip','install','--disable-pip-version-check','-r','requirements-lock.txt')
Invoke-Checked -Executable $taskPython -Arguments @('-m','pip','check')
$taskBootstrap=@('scripts/bootstrap_runtime.py')
if ($PostgreSQLArchive) { $taskBootstrap+=@('--archive',$PostgreSQLArchive) }
Invoke-Checked -Executable $taskPython -Arguments $taskBootstrap
$taskNative=@('scripts/native_packages.py')
if ($BuildNative) {
    if (-not $Compiler -or -not $Sdk) { throw '-BuildNative requires -Compiler and -Sdk' }
    $taskNative+=@('--build','--compiler',$Compiler,'--sdk',$Sdk)
}
Invoke-Checked -Executable $taskPython -Arguments $taskNative
Invoke-Checked -Executable $taskPython -Arguments @('-c','from bench.native_temp_stage_probe import source_identity,install; before=source_identity(); install(); assert source_identity()==before')
Invoke-Checked -Executable $taskPython -Arguments @('scripts/verify_upstream.py')
Invoke-Checked -Executable $taskPython -Arguments @('scripts/audit_release.py')
Write-Host 'Setup verified. Use scripts/reproduce.ps1 for a fresh paired benchmark.'
