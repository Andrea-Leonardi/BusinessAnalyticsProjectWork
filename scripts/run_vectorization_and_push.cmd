@echo off
setlocal

set "PROJECT_ROOT=%~dp0.."
pushd "%PROJECT_ROOT%" >nul

set "DVC_EXE=%PROJECT_ROOT%\.venv\Scripts\dvc.exe"

if not exist "%DVC_EXE%" (
    echo DVC non trovato in .venv\Scripts\dvc.exe
    echo Esegui prima .\scripts\bootstrap_venv.ps1 e configura DVC.
    popd >nul
    exit /b 1
)

echo [1/2] Rigenero i file di vectorization con DVC...
"%DVC_EXE%" repro vectorize_articles
if errorlevel 1 (
    echo Errore durante dvc repro.
    popd >nul
    exit /b 1
)

echo [2/2] Carico gli output tracciati su Google Drive...
"%DVC_EXE%" push
if errorlevel 1 (
    echo Errore durante dvc push.
    popd >nul
    exit /b 1
)

echo Operazione completata.
popd >nul
exit /b 0
