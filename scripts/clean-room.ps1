param([string]$Ref='HEAD',[string]$PythonExe='python')
$ErrorActionPreference='Stop'
$taskRoot=Split-Path -Parent $PSScriptRoot
New-Item -ItemType Directory -Force (Join-Path $taskRoot '.runtime') | Out-Null
$taskPreviousPythonPath=$env:PYTHONPATH
$taskRunner=(Get-Command $PythonExe -ErrorAction Stop).Source
function Invoke-CleanCommand {
    param([string]$Executable,[string[]]$CommandArguments)
    # Merge native stderr in the child. Windows PowerShell otherwise treats
    # normal pip notices/unittest progress as terminating errors when logged.
    & $taskRunner -c 'import subprocess,sys; sys.exit(subprocess.call(sys.argv[1:],stderr=subprocess.STDOUT))' $Executable @CommandArguments
    if ($LASTEXITCODE -ne 0) { throw "Clean-room command failed: $Executable" }
}
$taskName='reproduce_'+[guid]::NewGuid().ToString('N').Substring(0,10)
$taskDestination=Join-Path $taskRoot ".runtime/$taskName"
$taskArchive=Join-Path $taskRoot ".runtime/$taskName.zip"
git -C $taskRoot archive --format=zip --output=$taskArchive $Ref
if ($LASTEXITCODE -ne 0) { throw 'Archive failed' }
Expand-Archive -LiteralPath $taskArchive -DestinationPath $taskDestination
Push-Location -LiteralPath $taskDestination
try {
    Invoke-CleanCommand -Executable $taskRunner -CommandArguments @('-m','venv','.venv')
    $taskFreshPython=Join-Path $taskDestination '.venv/Scripts/python.exe'
    Invoke-CleanCommand -Executable $taskFreshPython -CommandArguments @('-m','pip','install','--disable-pip-version-check','-r','requirements-lock.txt')
    Invoke-CleanCommand -Executable $taskFreshPython -CommandArguments @('scripts/verify_upstream.py')
    Invoke-CleanCommand -Executable $taskFreshPython -CommandArguments @('-m','pytest','tests/test_recipes.py','-q')
    $env:PYTHONPATH=Join-Path $taskDestination 'app'
    Invoke-CleanCommand -Executable $taskFreshPython -CommandArguments @('-m','unittest','discover','-s','app/tests','-v')
} finally {
    if ($null -eq $taskPreviousPythonPath) { Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue }
    else { $env:PYTHONPATH=$taskPreviousPythonPath }
    Pop-Location
}
Write-Host "Clean-room source/dependency regression passed for $Ref in $taskDestination"
