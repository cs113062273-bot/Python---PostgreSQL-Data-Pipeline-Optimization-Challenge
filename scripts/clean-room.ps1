param([string]$Ref='HEAD',[string]$PythonExe='python')
$ErrorActionPreference='Stop'
$taskRoot=Split-Path -Parent $PSScriptRoot
New-Item -ItemType Directory -Force (Join-Path $taskRoot '.runtime') | Out-Null
$taskPreviousPythonPath=$env:PYTHONPATH
$taskName='reproduce_'+[guid]::NewGuid().ToString('N').Substring(0,10)
$taskDestination=Join-Path $taskRoot ".runtime/$taskName"
$taskArchive=Join-Path $taskRoot ".runtime/$taskName.zip"
git -C $taskRoot archive --format=zip --output=$taskArchive $Ref
if ($LASTEXITCODE -ne 0) { throw 'Archive failed' }
Expand-Archive -LiteralPath $taskArchive -DestinationPath $taskDestination
Push-Location -LiteralPath $taskDestination
try {
    & $PythonExe -m venv .venv
    & .venv/Scripts/python.exe -m pip install -r requirements-lock.txt
    if ($LASTEXITCODE -ne 0) { throw 'Clean-room dependency install failed' }
    & .venv/Scripts/python.exe scripts/verify_upstream.py
    if ($LASTEXITCODE -ne 0) { throw 'Upstream integrity failed' }
    & .venv/Scripts/python.exe -m pytest tests/test_recipes.py -q
    if ($LASTEXITCODE -ne 0) { throw 'Recipe tests failed' }
    $env:PYTHONPATH=Join-Path $taskDestination 'app'
    & .venv/Scripts/python.exe -m unittest discover -s app/tests -v
    if ($LASTEXITCODE -ne 0) { throw 'Organizer regression failed' }
} finally {
    if ($null -eq $taskPreviousPythonPath) { Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue }
    else { $env:PYTHONPATH=$taskPreviousPythonPath }
    Pop-Location
}
Write-Host "Clean-room source/dependency regression passed for $Ref in $taskDestination"
