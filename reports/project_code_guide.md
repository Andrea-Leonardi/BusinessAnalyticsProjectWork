# Project Code Guide

This file is a quick reference for the main project scripts.
The goal is to make the pipeline easy to understand after some time away from the codebase.


## Full Pipeline

The project runs in this order:

0. `src/0.FMP_runPipeline.py`
1. `src/1.FMP_companySelection.py`
2. `src/2.priceDataGathering.py`
3. `src/3.FMP_financialsDataGathering.py`
4. `src/4.FMP_financialsDataProcessing.py`
5. `src/5.FMP_dataAnalysis.py`
6. `src/6.FMP_dataMerge.py`

`src/config.py` defines the shared paths used by all scripts.


## `src/0.FMP_runPipeline.py`

Purpose:

- Run the main production pipeline from start to finish
- Clear the old company-level CSV files before rebuilding them

What it currently does:

- Deletes existing CSV files inside:
  - `data/dataExtraction/singleCompanyData/prices/`
  - `data/dataExtraction/singleCompanyData/financials/`
  - `data/dataExtraction/singleCompanyData/fulldata/`
- Runs these scripts in sequence:
  1. `src/1.FMP_companySelection.py`
  2. `src/2.priceDataGathering.py`
  3. `src/3.FMP_financialsDataGathering.py`
  4. `src/4.FMP_financialsDataProcessing.py`
  5. `src/6.FMP_dataMerge.py`

Important note:

- This runner intentionally does not execute `src/5.FMP_dataAnalysis.py`
- It is meant for rebuilding the core datasets, not for quality-check analysis


## `src/config.py`

This file contains the main project paths.

Main outputs:

- `data/dataExtraction/enterprises.csv`: selected company universe
- `data/dataExtraction/allPriceData.csv`: aggregated weekly price data
- `data/dataExtraction/financialsDataRaw.csv`: raw quarterly accounting data downloaded from FMP
- `data/dataExtraction/financialsData.csv`: final processed weekly financial dataset
- `data/dataExtraction/fulldata.csv`: final merged weekly dataset with prices and financial features
- `data/dataExtraction/fulldata_ml.csv`: ML-ready version of the merged dataset without date and ticker columns
- `data/dataExtraction/singleCompanyData/prices/`: one weekly price file per company
- `data/dataExtraction/singleCompanyData/financials/`: one processed financial file per company
- `data/dataExtraction/singleCompanyData/fulldata/`: one merged weekly file per company
- `data/modeling/`: empty folder reserved for future modeling outputs


## `src/1.FMP_companySelection.py`

Purpose:

- Download the initial company universe from the FMP screener
- Keep only active US stocks
- Keep the largest companies by sector

Main logic:

- Calls the FMP `company-screener` endpoint
- Removes duplicate companies by keeping the line with the highest `marketCap`
- Manually excludes a few tickers with downstream data issues:
  - `GEV`
  - `TBB`
  - `RCB`
  - `PLTR`
  - `HSBC`
  - `BAC`
  - `JPM`
  - `WFC`
- Keeps only `NASDAQ` and `NYSE`
- Keeps the top 10 companies by market cap inside each sector
- Saves the result to `data/dataExtraction/enterprises.csv`

Important output columns:

- `Ticker`
- `companyName`
- `sector`
- `industry`
- `marketCap`


## `src/2.priceDataGathering.py`

Purpose:

- Download daily stock prices from Yahoo Finance
- Convert them into a weekly calendar shared across the project

Main logic:

- Starts from the company list in `enterprises.csv`
- Downloads daily prices from a short pre-sample window before `2021-01-01`
- Keeps both:
  - `ClosePrice`
  - `AdjClosePrice`
- Maps each trading day to its `WeekEndingFriday`
- Keeps the last available trading day in each week
- Creates a few short lags such as `ClosePrice_t-1` and `ClosePrice_t-2`
- Creates one forward weekly price column, `ClosePrice_t+1`, that can later be
  used as a predictive target
- Creates a binary direction target, `AdjClosePrice_t+1_Up`, equal to `1`
  when next week's adjusted close is above the current one
- Saves one company file and one aggregated file

Generated price columns:

- `ClosePrice`
- `ClosePrice_t-1`
- `ClosePrice_t-2`
- `ClosePrice_t+1`
- `AdjClosePrice`
- `AdjClosePrice_t-1`
- `AdjClosePrice_t-2`
- `AdjClosePrice_t+1`
- `AdjClosePrice_t+1_Up`

Why this file matters:

- The weekly price calendar becomes the master calendar used later in the financial processing step
- The processing script aligns quarterly FMP statements to this weekly grid
- The extra pre-2021 price history is kept so weekly lags are already available
  when the final analysis window starts in 2021


## `src/3.FMP_financialsDataGathering.py`

Purpose:

- Download raw quarterly financial statements from FMP
- Keep only the fields needed by the project
- Merge them into one raw accounting dataset

Endpoints used:

- `income-statement`
- `balance-sheet-statement`
- `cash-flow-statement`
- `enterprise-values`

What one row in `financialsDataRaw.csv` represents:

- One quarterly observation for one company
- That row combines metadata, income-statement values, balance-sheet values, cash-flow values, and market cap

How the script works:

1. Load tickers from `enterprises.csv`
2. For each ticker, download up to 60 quarterly observations from each endpoint
3. Convert date columns to datetime
4. Remove duplicates, keeping the newest filing for each quarter
5. Select only the requested fields
6. Merge the four sources into one company-level DataFrame
7. Concatenate all companies into `financialsDataRaw.csv`

Rate-limit handling:

- The downloader pauses between API calls to avoid hitting FMP too aggressively
- If FMP returns `429 Too Many Requests`, the script retries the same endpoint a few times with a progressively longer wait
- This makes the raw download slower, but much more stable when the universe contains many tickers

Important point about column selection:

- The script first tries the primary field name
- It uses a fallback only when that fallback was explicitly allowed in the field map
- If a fallback is used, the script prints a warning
- If the expected field is missing, the script saves `NA` and prints a warning

This was done to avoid silently using a similar but not fully equivalent field.

Examples:

- `netIncome` prefers `netIncome` and falls back to `bottomLineNetIncome`
- `totalStockholdersEquity` prefers `totalStockholdersEquity` and falls back to `totalEquity`
- `marketCap` prefers `marketCapitalization` and falls back to `marketCap`
- `totalCurrentAssets` and `totalCurrentLiabilities` now use only their exact names

Main output columns in the raw file:

- Metadata:
  - `requested_symbol`
  - `symbol`
  - `date`
  - `fiscalYear`
  - `period`
  - `filingDate`
  - `acceptedDate`
  - `reportedCurrency`
  - `cik`
- Income statement:
  - `revenue`
  - `grossProfit`
  - `operatingIncome`
  - `netIncome`
  - `interestExpense`
  - `weightedAverageShsOut`
  - `weightedAverageShsOutDil`
- Balance sheet:
  - `totalAssets`
  - `totalStockholdersEquity`
  - `totalCurrentAssets`
  - `totalCurrentLiabilities`
  - `totalDebt`
  - `cashAndCashEquivalents`
- Cash flow:
  - `operatingCashFlow`
  - `capitalExpenditure`
  - `freeCashFlow`
- Enterprise values:
  - `marketCap`


## `src/4.FMP_financialsDataProcessing.py`

Purpose:

- Transform the raw quarterly FMP data into a weekly company-level dataset
- Align accounting data to the weekly price calendar
- Create the financial ratios used in the project

High-level logic:

1. Optionally rerun the raw FMP downloader
2. Load `financialsDataRaw.csv`
3. Process one company at a time
4. Build an effective public date for each statement using `acceptedDate`, `filingDate`, and quarter-end fallback logic
5. Align that public date to the first Friday on or after the release
6. If needed, seed the first weekly row with the latest statement already known before the price sample starts
7. Forward-fill the latest known accounting information
8. Create weekly and quarterly lagged variables
9. Trim the final exported dataset to the main analysis window starting in 2021
10. Save one processed company file and one aggregated processed file

Important design choice:

- The script now calculates the accounting ratios on true quarterly statement dates first
- Only after that does it map them to the weekly price calendar
- Quarter-end `date` is not used directly as the market-availability date anymore
- The preferred release timestamp is `acceptedDate`, with fallback to `filingDate`
- If both are missing or suspiciously earlier than the quarter-end date, the script falls back to the latest available value among `date`, `filingDate`, and `acceptedDate`
- The weekly alignment uses the first Friday on or after that effective public date, so the feature never enters the panel before the market could observe it
- This makes the logic easier to interpret and allows TTM values to exist from the start of the weekly sample when enough older quarters were downloaded
- When an older pre-sample statement exists, the script uses it to seed the first weekly row so the dataset does not start with a long empty block before the first in-sample release
- The script also keeps a short pre-2021 window while building weekly lags, then trims the final exported panel back to the main 2021+ sample
- The script applies a zero-value cleaning step for suspicious provider-style zeros in `capitalExpenditure` and `freeCashFlow`
- It also flags isolated `totalDebt = 0` quarters surrounded by positive debt values and treats the derived debt ratio as missing in those segments

How the TTM values are built:

- TTM is calculated directly on quarterly statement rows
- For the flow variables, the script uses a rolling sum of the last 4 quarters
- `GrossProfitability_TTM` and `ROA_TTM` use average assets over one year rather than a single-quarter asset denominator
- `capitalExpenditure` is standardized to a consistent outflow sign before building `InvestmentIntensity`, which is then stored as a positive spending ratio
- Examples of flow variables:
  - `revenue`
  - `grossProfit`
  - `operatingIncome`
  - `netIncome`
  - `operatingCashFlow`
  - `freeCashFlow`

Why TTM is useful:

- It reduces seasonal patterns in variables such as earnings, margins, and cash flow
- It lets you compare the normal quarterly ratio with a smoother TTM version

How market cap is handled:

- Raw quarterly `marketCap` is downloaded from FMP
- During weekly processing, that quarterly market cap is updated week by week using weekly `ClosePrice`
- This means market-based ratios do not stay flat between quarters

In practice:

- Accounting numerators and denominators remain fixed until a new statement arrives
- Market-based ratios move weekly because the market cap is updated with price changes

Main output ratios:

- `QuarterlyReleased`
- `BookToMarket`
- `MarketCap`
- `GrossProfitability`
- `GrossProfitability_TTM`
- `OperatingMargin`
- `OperatingMargin_TTM`
- `ROA`
- `ROA_TTM`
- `AssetGrowth`
- `InvestmentIntensity`
- `Accruals`
- `Accruals_TTM`
- `DebtToAssets`
- `WorkingCapitalScaled`
- `FreeCashFlowYield`
- `FreeCashFlowYield_TTM`
- `EarningsYield`
- `EarningsYield_TTM`

Excluded from the current analysis:

- `InterestCoverage`
- `InterestCoverage_TTM`
- `ROE`
- `ROE_TTM`
- `IncomeQuality`
- `IncomeQuality_TTM`
- `CashRatio`
- `InvestmentIntensity_TTM`
- All lagged variants that would depend on the excluded feature families

These variables were removed from the final analysis set because they create
structural missing values, unstable scaling, or weak cross-sector comparability
in the current sample.

In particular, the `ROE` family was removed even though the textbook formula is
correct, because negative or buyback-compressed equity makes the ratio hard to
interpret and would otherwise eliminate some companies entirely from the
complete-case ML dataset.

Lagged output columns:

- Market-based variables also receive weekly lags:
  - `_L1W`
  - `_L2W`
- Fundamental variables also receive quarterly lags:
  - `_L1Q`
  - `_L2Q`

Why the lag design is split:

- `BookToMarket`, `MarketCap`, `FreeCashFlowYield`, `FreeCashFlowYield_TTM`, `EarningsYield`, and `EarningsYield_TTM` can move every week because they depend on market cap
- Their lagged versions are therefore computed as one-week and two-week shifts
- The other accounting ratios move only when a new quarterly statement becomes public
- Their lagged versions are therefore computed on release weeks after the zero-cleaning step, then carried forward on the weekly calendar
- Quarterly lag columns are appended in one batch during processing to avoid pandas fragmentation warnings and keep the script faster to run

Important distinction:

- Most accounting ratios are stepwise and change only when a new quarterly statement becomes public
- `BookToMarket`, `FreeCashFlowYield`, and `EarningsYield` can also move between statements because they depend on market cap

About `QuarterlyReleased`:

- This is a binary weekly flag
- It is equal to `1` when a newly downloaded quarterly FMP observation is mapped to that week
- It is equal to `0` in all the other weeks
- Weeks filled only by forward fill remain `0`


## `src/5.FMP_dataAnalysis.py`

Purpose:

- Perform quick checks on the processed dataset

What it currently does:

- Summarizes missing values and explicit zeros for each company-level
  processed financial file, excluding the `QuarterlyReleased` flag
- Reports both absolute counts and percentages for missing values and zeros
- Loads `fulldata_ml.csv`, computes the feature correlation matrix, keeps only
  variables that show at least one absolute correlation of `0.20` or more, and
  plots a corrplot-style heatmap for that reduced set
- Plots a target-centric bar chart of the strongest feature correlations with
  `AdjClosePrice_t+1_Up`
- Plots a compact correlation heatmap around the target using only the top
  target-related features, which is easier to read than a full corrplot and
  also prints the correlation values inside each cell
- Handles missing company files or a missing `fulldata_ml.csv` gracefully by
  printing a message instead of breaking the whole script

This script is more of a sanity-check script than a final analysis pipeline.


## `src/6.FMP_dataMerge.py`

Purpose:

- Merge the weekly price files with the weekly processed financial files
- Create one final company-level dataset per ticker
- Create one large final dataset with all companies stacked together

What it currently does:

- Loads the ticker list from `enterprises.csv`
- Reads `data/dataExtraction/singleCompanyData/prices/{ticker}Prices.csv`
- Reads `data/dataExtraction/singleCompanyData/financials/{ticker}Financials.csv`
- Standardizes the ticker column so both files use `Ticker`
- Merges the two sources on:
  - `WeekEndingFriday`
  - `Ticker`
- Reorders the merged columns so the two target columns,
  `AdjClosePrice_t+1` and `AdjClosePrice_t+1_Up`, appear immediately after
  `WeekEndingFriday` and `Ticker`
- Saves one company-level merged file to `data/dataExtraction/singleCompanyData/fulldata/{ticker}data.csv`
- Concatenates all company files into `data/dataExtraction/fulldata.csv`
- Creates `data/dataExtraction/fulldata_ml.csv` by dropping `WeekEndingFriday` and `Ticker`
  from the aggregated merged dataset
- Before exporting `data/dataExtraction/fulldata_ml.csv`, it also drops every row that still
  contains at least one missing value

Why this file matters:

- It creates the final modeling table where market data and financial features live in the same weekly dataset
- It preserves the weekly calendar already aligned in the earlier steps of the pipeline
- It also exports an ML-ready matrix without identifier columns for direct use in predictive models
- The ML-ready export is a complete-case dataset, because rows with any missing
  values are removed before saving


## Suggested Use When Returning To The Project

If you want to quickly understand what is happening, read the files in this order:

1. `reports/project_code_guide.md`
2. `src/config.py`
3. `src/3.FMP_financialsDataGathering.py`
4. `src/4.FMP_financialsDataProcessing.py`

That order usually gives the fastest overview of:

- where the data comes from
- how the raw quarterly dataset is built
- how the weekly processed dataset is created
- where each final variable comes from
