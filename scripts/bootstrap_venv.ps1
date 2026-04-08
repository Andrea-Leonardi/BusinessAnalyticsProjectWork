param(
    [switch]$Recreate,
    [switch]$RegisterKernel,
    [switch]$SkipDependencyInstall
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$VenvPath = Join-Path $ProjectRoot ".venv"
$RequirementsPath = Join-Path $ProjectRoot "requirements.txt"
$PythonCandidates = @()

$LocalPython = Join-Path $env:LOCALAPPDATA "Programs\Python\Python313\python.exe"
if (Test-Path $LocalPython) {
    $PythonCandidates += $LocalPython
}

$PythonCommand = Get-Command python -ErrorAction SilentlyContinue
if ($PythonCommand) {
    $PythonCandidates += $PythonCommand.Source
}

$PythonCandidates = $PythonCandidates | Select-Object -Unique

if (-not $PythonCandidates) {
    throw "Python non trovato. Installa Python 3.13 oppure aggiungi python al PATH."
}

$PythonExe = $PythonCandidates | Select-Object -First 1

if ($Recreate -and (Test-Path $VenvPath)) {
    Remove-Item -Recurse -Force $VenvPath
}

if (-not (Test-Path $VenvPath)) {
    & $PythonExe -m venv $VenvPath
}

$VenvPython = Join-Path $VenvPath "Scripts\python.exe"
$NvidiaSmiCommand = Get-Command nvidia-smi -ErrorAction SilentlyContinue

& $VenvPython -m pip install --upgrade pip setuptools wheel

if (-not $SkipDependencyInstall) {
    & $VenvPython -m pip install -r $RequirementsPath

    if ($NvidiaSmiCommand) {
        Write-Host "NVIDIA GPU rilevata: installo PyTorch con supporto CUDA 12.8..." -ForegroundColor Cyan
        & $VenvPython -m pip install --upgrade torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu128
    }
    else {
        Write-Host "Nessuna NVIDIA GPU rilevata: installo PyTorch CPU..." -ForegroundColor Cyan
        & $VenvPython -m pip install --upgrade torch torchvision torchaudio
    }
}

if ($RegisterKernel) {
    & $VenvPython -m ipykernel install --user --name "BusinessAnalyticsProjectWork" --display-name "Python (.venv) BusinessAnalyticsProjectWork"
}

Write-Host ""
Write-Host "Environment ready:" -ForegroundColor Green
Write-Host "  $VenvPython"
Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. In VS Code select .venv\\Scripts\\python.exe as interpreter."
Write-Host "  2. Open the notebook again and pick the .venv kernel."
