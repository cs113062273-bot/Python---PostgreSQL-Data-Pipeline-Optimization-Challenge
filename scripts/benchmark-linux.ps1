# Windows client driving the unchanged official generator against a Linux server.
# Supply credentials through libpq/.pgpass or PG_KAGGLE_CHALLENGE_PASS.
param(
    [Parameter(Mandatory=$true)][string]$DatabaseHost,
    [Parameter(Mandatory=$true)][string]$DatabaseUser,
    [Parameter(Mandatory=$true)][string]$SshUser,
    [int]$SshPort=22,
    [int]$Rows=100000
)
$ErrorActionPreference='Stop'
python app/generate_database.py --rows $Rows --host $DatabaseHost --ssh-port $SshPort --ssh-user $SshUser --user $DatabaseUser
if ($LASTEXITCODE -ne 0) { throw 'Official generator failed' }
Write-Host 'Generator finished. Launch the actual Windows GUI and configure the same Linux/SSH endpoint.'
python launch.py --mode baseline
