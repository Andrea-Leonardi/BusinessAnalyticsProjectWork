param(
    [Parameter(Mandatory = $true)]
    [string]$BucketName,
    [string]$BucketPath = "dvc-storage",
    [string]$RemoteName = "gcs_storage",
    [string]$ServiceAccountJsonPath,
    [switch]$RunVectorizationStage,
    [switch]$PushAfterRepro
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$VenvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$DvcExe = Join-Path $ProjectRoot ".venv\Scripts\dvc.exe"
$GcsUrl = "gs://$BucketName"

if ($BucketPath) {
    $CleanBucketPath = $BucketPath.Trim("/")
    if ($CleanBucketPath) {
        $GcsUrl = "$GcsUrl/$CleanBucketPath"
    }
}

if (-not (Test-Path $VenvPython)) {
    throw "Virtual environment non trovato in .venv. Esegui prima .\scripts\bootstrap_venv.ps1."
}

if (-not (Test-Path $DvcExe)) {
    Write-Host "DVC non trovato nella .venv: installo dvc e dvc-gs..." -ForegroundColor Cyan
    & $VenvPython -m pip install dvc dvc-gs
}

if (-not (Test-Path $DvcExe)) {
    throw "Installazione DVC non riuscita. Verifica la connessione e riprova."
}

$DvcDir = Join-Path $ProjectRoot ".dvc"
if (-not (Test-Path $DvcDir)) {
    & $DvcExe init
}

& $DvcExe remote add -f -d $RemoteName $GcsUrl

if ($ServiceAccountJsonPath) {
    & $DvcExe remote modify --local $RemoteName credentialpath $ServiceAccountJsonPath
    Write-Host "Configurato service account locale per il remote '$RemoteName'." -ForegroundColor Cyan
}
else {
    Write-Host "Remote GCS configurato senza credentialpath locale." -ForegroundColor Cyan
    Write-Host "Usa 'gcloud auth application-default login' oppure passa -ServiceAccountJsonPath." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "DVC configurato con remote Google Cloud Storage '$RemoteName'." -ForegroundColor Green
Write-Host "Remote path: $GcsUrl"

if ($RunVectorizationStage) {
    & $DvcExe repro vectorize_articles
}

if ($PushAfterRepro) {
    & $DvcExe push
}
