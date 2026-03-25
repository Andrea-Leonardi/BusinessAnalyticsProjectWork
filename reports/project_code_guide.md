# Project Code Guide

This file is a quick reference for the main project scripts.
The goal is to make the pipeline easy to understand after some time away from the codebase.


## Full Pipeline

The project runs in this order:

1. `src/1.FMP_companySelection.py`
2. `src/2.priceDataGathering.py`
3. `src/3.FMP_financialsDataGathering.py`
4. `src/4.FMP_financialsDataProcessing.py`
5. `src/5.FMP_dataAnalysis.py`

`src/config.py` defines the shared paths used by all scripts.


## `src/config.py`

This file contains the main project paths.

Main outputs:

- `data/enterprises.csv`: selected company universe
- `data/allPriceData.csv`: aggregated weekly price data
- `data/financialsDataRaw.csv`: raw quarterly accounting data downloaded from FMP
- `data/financialsData.csv`: final processed weekly financial dataset
- `data/singleCompanyData/prices/`: one weekly price file per company
- `data/singleCompanyData/financials/`: one processed financial file per company


## `src/1.FMP_companySelection.py`

Purpose:

- Download the initial company universe from the FMP screener
- Keep only active US stocks
- Keep the largest companies by sector

Main logic:

- Calls the FMP `company-screener` endpoint
- Removes duplicate companies by keeping the line with the highest `marketCap`
- Keeps only `NASDAQ` and `NYSE`
- Keeps the top 10 companies by market cap inside each sector
- Saves the result to `data/enterprises.csv`

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
- Downloads daily prices from `2021-01-01`
- Keeps both:
  - `ClosePrice`
  - `AdjClosePrice`
- Maps each trading day to its `WeekEndingFriday`
- Keeps the last available trading day in each week
- Creates a few short lags such as `ClosePrice_t-1` and `ClosePrice_t-2`
- Creates one forward weekly price column, `ClosePrice_t+1`, that can later be
  used as a predictive target
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

Why this file matters:

- The weekly price calendar becomes the master calendar used later in the financial processing step
- The processing script aligns quarterly FMP statements to this weekly grid


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
4. Align quarterly statement dates to the nearest Friday
5. Map each company to its weekly price calendar
6. If needed, seed the first weekly row with the latest statement already known before the price sample starts
7. Forward-fill the latest known accounting information
8. Save one processed company file and one aggregated processed file

Important design choice:

- The script now calculates the accounting ratios on true quarterly statement dates first
- Only after that does it map them to the weekly price calendar
- This makes the logic easier to interpret and allows TTM values to exist from the start of the weekly sample when enough older quarters were downloaded
- When an older pre-sample statement exists, the script uses it to seed the first weekly row so the dataset does not start with a long empty block before the first in-sample release

How the TTM values are built:

- TTM is calculated directly on quarterly statement rows
- For the flow variables, the script uses a rolling sum of the last 4 quarters
- Examples of flow variables:
  - `revenue`
  - `grossProfit`
  - `operatingIncome`
  - `netIncome`
  - `interestExpense`
  - `capitalExpenditure`
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
- `ROE`
- `ROE_TTM`
- `AssetGrowth`
- `InvestmentIntensity`
- `InvestmentIntensity_TTM`
- `Accruals`
- `Accruals_TTM`
- `IncomeQuality`
- `IncomeQuality_TTM`
- `DebtToAssets`
- `InterestCoverage`
- `InterestCoverage_TTM`
- `CashRatio`
- `WorkingCapitalScaled`
- `FreeCashFlowYield`
- `FreeCashFlowYield_TTM`
- `EarningsYield`
- `EarningsYield_TTM`

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
- Their lagged versions are therefore computed on the statement timeline first, then carried forward on the weekly calendar

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
- Inspect date coverage and visually inspect example price series

What it currently does:

- Loads `financialsData.csv`
- Checks first and last available date by ticker
- Highlights suspicious end dates
- Plots an example weekly price series for one selected company

This script is more of a sanity-check script than a final analysis pipeline.


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
