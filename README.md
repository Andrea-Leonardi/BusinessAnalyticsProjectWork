# BusinessAnalyticsProjectWork

Repository for the Business Analytics project code, data, and reports.

## Environment Setup

This project now standardizes on a single local virtual environment named `.venv`.
Do not create additional `venv`, `env`, or notebook-specific environments inside the repository.

### Bootstrap the project

From the repository root in PowerShell:

```powershell
.\scripts\bootstrap_venv.ps1
```

If you want to recreate the environment from scratch:

```powershell
.\scripts\bootstrap_venv.ps1 -Recreate
```

If you also want to register a dedicated Jupyter kernel:

```powershell
.\scripts\bootstrap_venv.ps1 -RegisterKernel
```

The bootstrap script:

1. uses the local Python installation
2. creates or reuses `.venv`
3. upgrades `pip`
4. installs the packages listed in [requirements.txt](/c:/Users/leoan/OneDrive/All/Andrea/Documenti/GitHub/BusinessAnalyticsProjectWork/requirements.txt)

The default environment excludes ad hoc notebook-only packages that are not part of the shared project workflow.
In particular, legacy `alpaca_trade_api` cells found in some notebook exports are not required for the main repository setup.

### VS Code and Jupyter

The repository includes [settings.json](/c:/Users/leoan/OneDrive/All/Andrea/Documenti/GitHub/BusinessAnalyticsProjectWork/.vscode/settings.json) that points VS Code to `.venv\Scripts\python.exe`.

If Jupyter still shows the wrong kernel:

1. open the Command Palette
2. run `Python: Select Interpreter`
3. choose `.venv\Scripts\python.exe`
4. reopen the notebook and select the `.venv` kernel

### Legacy environments

If you still see folders such as `.venv_old`, treat them as temporary backups only.
Once the new `.venv` works and you have reinstalled the required packages, you can delete those legacy folders locally.

## Repository Structure

### `reports/`

Project documentation and CRISP-DM deliverables.

### `data/`

Raw and processed datasets used by the project.
Large generated artifacts such as vectorized news matrices should stay out of Git and be versioned with DVC.

### `notebooks/`

Exploratory notebooks and Colab-derived scripts.
Some notebook fragments still contain Colab-specific commands such as `!pip install` or `google.colab`; these are not part of the standard local setup.
Some exported notebook code also references legacy packages that are intentionally not included in the default environment.

### `src/`

Reusable project scripts for data extraction, news processing, and modeling.

## Data Versioning With DVC

Heavy generated CSV files are not meant to be committed directly to GitHub.
The repository is prepared to version those artifacts with DVC and store them on Google Cloud Storage.

The first stage configured in [dvc.yaml](/c:/Users/leoan/OneDrive/All/Andrea/Documenti/GitHub/BusinessAnalyticsProjectWork/dvc.yaml) is `vectorize_articles`, which tracks the CSV outputs produced by [2. vectorization_articles.py](/c:/Users/leoan/OneDrive/All/Andrea/Documenti/GitHub/BusinessAnalyticsProjectWork/src/modeling_news/2.%20vectorization_articles.py).

### One-time setup

```powershell
.\scripts\setup_dvc_gcs.ps1 -BucketName "<YOUR_GCS_BUCKET>" -BucketPath "dvc-storage"
```

This script:

1. installs `dvc` and `dvc-gs` into `.venv` if needed
2. runs `dvc init` if the repository is not initialized yet
3. configures a default Google Cloud Storage remote

### Rebuild and push the vectorization outputs

```powershell
.\scripts\setup_dvc_gcs.ps1 -BucketName "<YOUR_GCS_BUCKET>" -BucketPath "dvc-storage" -RunVectorizationStage -PushAfterRepro
```

If you prefer a service account instead of `gcloud auth application-default login`:

```powershell
.\scripts\setup_dvc_gcs.ps1 -BucketName "<YOUR_GCS_BUCKET>" -BucketPath "dvc-storage" -ServiceAccountJsonPath "C:\path\service-account.json"
```

You can also run the DVC commands manually:

```powershell
.\.venv\Scripts\dvc.exe repro vectorize_articles
.\.venv\Scripts\dvc.exe push
.\.venv\Scripts\dvc.exe pull
```

Or use the wrapper script:

```powershell
.\scripts\run_vectorization_and_push.cmd
```
