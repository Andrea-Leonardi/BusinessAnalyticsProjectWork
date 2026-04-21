# BusinessAnalyticsProjectWork

End-to-end pipeline for building a weekly multi-modal dataset for one-week-ahead stock direction prediction (`t+1`) by combining:

- market data from Yahoo Finance;
- company fundamentals from Financial Modeling Prep;
- financial news from Alpaca;
- NLP features derived from FinBERT.

The repository includes both the operational pipeline used to generate the final datasets and exploratory material, reports, and saved modeling artifacts.

## Project goal

The goal is to build a weekly panel dataset on a universe of **110 companies** selected as the **top 10 companies in each of 11 sectors**, with an analysis window starting from **2021-01-01**, to predict the binary target:

`AdjClosePrice_t+1_Up = 1` if the next week's adjusted price is higher than the current week's adjusted price, otherwise `0`.

## Current repository status

The most coherent and reusable parts of the codebase today are:

- `src/config.py`: central path configuration.
- `src/1.dataExtraction/`: price and fundamentals dataset construction.
- `src/2.newsExtraction/`: news maintenance, text analysis, and weekly aggregation.
- `src/4.modeling/`: classical supervised modeling pipeline.
- `src/6.evaluation/`: support scripts for evaluation and sector-level predictions.

The following folders mainly contain experimental or support material:

- `notebooks/`
- `src/3.modeling_news/`
- `src/5.relationalDatabase/`
- `reports/`

## Main structure

```text
.
|-- data/
|   |-- dataExtraction/
|   |-- newsExtraction/
|   `-- modeling/
|-- notebooks/
|-- reports/
|-- src/
|   |-- config.py
|   |-- 1.dataExtraction/
|   |-- 2.newsExtraction/
|   |-- 3.modeling_news/
|   |-- 4.modeling/
|   |-- 5.relationalDatabase/
|   `-- 6.evaluation/
|-- requirements.txt
`-- README.md
```

## Data pipeline

### 1. Data extraction

Entry point: `src/1.dataExtraction/rundataExtraction.py`

Flow:

1. build the company universe using FMP and rank firms by historical market cap near `2021-01-04`;
2. download daily prices with `yfinance` and aggregate them to the weekly `WeekEndingFriday` calendar;
3. download quarterly financial statements from FMP;
4. transform them into fundamental features and lagged variables;
5. build the final merged price + fundamentals dataset.

Main outputs:

- `data/dataExtraction/enterprises.csv`
- `data/dataExtraction/companySelectionUniverse.csv`
- `data/dataExtraction/allPriceData.csv`
- `data/dataExtraction/financialsDataRaw.csv`
- `data/dataExtraction/financialsData.csv`
- `data/dataExtraction/fulldata.csv`

### 2. News extraction and aggregation

Full entry point: `src/2.newsExtraction/runFullPipeline.py`

This runner executes:

1. `src/1.dataExtraction/rundataExtraction.py`
2. `src/2.newsExtraction/pipeline/3.newsMaintenance.py`
3. `src/2.newsExtraction/pipeline/4.textAnalysis.py`
4. `src/2.newsExtraction/pipeline/5.weeklyNewsAggregation.py`

Important operational notes:

- `3.newsMaintenance.py` can also initialize `newsArticles.csv` from scratch if the file does not exist.
- `1.newsDataGathering.py` and `2.missingSummaryImputation.py` are still available as standalone utilities, but the current workflow relies mainly on `3.newsMaintenance.py`.
- `4.textAnalysis.py` works at article level and uses a persistent cache in `textAnalysisCache.csv`.
- `5.weeklyNewsAggregation.py` aligns news features to the `fulldata.csv` calendar, creates the final merged dataset, and produces the modeling dataset.

Main outputs:

- `data/newsExtraction/raw_news_data/*.csv`
- `data/newsExtraction/newsArticles.csv`
- `data/newsExtraction/textAnalysisCache.csv`
- `data/newsExtraction/textAnalysis.csv`
- `data/newsExtraction/textAnalysisWeekly.csv`
- `data/dataExtraction/fulldata_with_news.csv`
- `data/modeling/modeling.csv`

### 3. Modeling

Entry point: `src/4.modeling/run_all_classic_models.py`

The pipeline compares several classification models:

- `null_model`
- `always_zero_model`
- `always_one_model`
- `lasso_model`
- `logistic_regression`
- `random_forest`
- `XGBoost`
- `neural network`

The current temporal split is:

- `2021-2024`: training
- `2025`: validation
- `2026`: test

The sector used by the pipeline is controlled in `src/4.modeling/classic_ML_model/split_data.py` through the `SECTOR_FILTER` constant.

Artifacts are saved under `src/4.modeling/classic_ML_model/<sector>/...`, including:

- trained models;
- `best_params.json`;
- `performance.json`;
- `orchestrator_results/model_comparison.json`;
- `orchestrator_results/model_comparison.csv`.

## Requirements

- Python **3.11** recommended
- Windows + PowerShell is the most consistent setup with the current repository
- internet access for downloads from FMP, Yahoo Finance, and Alpaca
- local cache for the Hugging Face models required by the text analysis step

## Environment setup

There is no `scripts/bootstrap_venv.ps1` script in the repository: the actual setup is currently manual.

From the project root:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

For GPU support on Windows, `requirements.txt` also documents the alternative installation path using the official PyTorch CUDA wheels.

## Credentials and external services

### Financial Modeling Prep

Used by the scripts under `src/1.dataExtraction/pipeline/`.

Current code status:

- the FMP key is currently defined directly inside the Python files;
- for proper project usage, you should replace it with your own key before running the pipeline.

### Alpaca News API

Used by the news scripts. The supported environment variables include at least:

```powershell
$env:ALPACA_API_KEY="..."
$env:ALPACA_SECRET_KEY="..."
```

Useful optional variables:

- `NEWS_IMPORT_START`
- `NEWS_IMPORT_END`
- `NEWS_IMPORT_WINDOW_DAYS`
- `NEWS_IMPORT_TICKER_OFFSET`
- `NEWS_IMPORT_TICKER_LIMIT`
- `NEWS_MAINTENANCE_TICKER`

### Hugging Face / NLP models

`src/2.newsExtraction/pipeline/4.textAnalysis.py` forces offline mode and uses `local_files_only=True`, so the required models must already be available in the local cache. The code currently uses:

- `ProsusAI/finbert`
- `SamLowe/roberta-base-go_emotions`
- `facebook/bart-large-mnli` only if zero-shot classification is enabled

If these models have not already been downloaded, the text analysis step will fail.

## Main commands

### Rebuild prices and fundamentals only

```powershell
.\.venv\Scripts\python.exe src\1.dataExtraction\rundataExtraction.py
```

### Run the full data + news + modeling-dataset pipeline

```powershell
.\.venv\Scripts\python.exe src\2.newsExtraction\runFullPipeline.py
```

### Run the classical modeling pipeline

```powershell
.\.venv\Scripts\python.exe src\4.modeling\run_all_classic_models.py
```

Before running the modeling pipeline, make sure that `data/modeling/modeling.csv` exists and that `SECTOR_FILTER` is set to the desired sector.

## Key datasets

| File | Content |
| --- | --- |
| `data/dataExtraction/enterprises.csv` | final selected company universe |
| `data/dataExtraction/allPriceData.csv` | weekly prices and technical features |
| `data/dataExtraction/financialsData.csv` | weekly aligned fundamental features |
| `data/dataExtraction/fulldata.csv` | final merged price + fundamentals dataset |
| `data/newsExtraction/newsArticles.csv` | article-level news dataset |
| `data/newsExtraction/textAnalysis.csv` | article-level NLP metrics |
| `data/newsExtraction/textAnalysisWeekly.csv` | weekly aggregated news features by `Ticker` and `WeekEndingFriday` |
| `data/dataExtraction/fulldata_with_news.csv` | `fulldata.csv` enriched with news features |
| `data/modeling/modeling.csv` | final cleaned dataset for ML models |

## Additional documentation

For methodology details and code notes:

- `reports/business_understanding.md`
- `reports/modeling.md`
- `reports/project_code_guide.md`
- `reports/evaluation.md`

## Known limitations

- some external credentials are still handled in a suboptimal way and should be moved out of the code;
- part of the repository still contains exported notebooks, legacy scripts, and experimental artifacts;
- the news pipeline depends on local availability of the Hugging Face models;
- the classical modeling pipeline requires an explicit change to `SECTOR_FILTER` to switch sectors.
