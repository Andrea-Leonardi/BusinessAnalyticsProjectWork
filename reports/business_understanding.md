# Define Business Case
# MarketPulse: Multi-Modal Stock Movement Predictor

## 1. Background & Problem Statement
For decades, quantitative analysts and financial institutions have attempted to predict the future behavior of financial markets. Traditional theoretical frameworks, such as the Capital Asset Pricing Model (CAPM) and the Efficient Market Hypothesis (EMH), often operate under the assumption that market participants are entirely rational and that asset prices fully reflect all available fundamental information. 

However, as demonstrated by the principles of **Behavioral Finance**, purely quantitative models fail to capture a critical driver of short-term market volatility: **Investor Sentiment**. Real-world market anomalies—such as retail-driven short squeezes or panic sell-offs triggered by breaking news—prove that collective emotions significantly influence stock movements. A prominent example is the **2021 GameStop (GME) short squeeze**, where retail investors on social media—driven entirely by collective sentiment and herd behavior rather than underlying fundamental valuation—pushed the stock price to unprecedented highs, completely defying traditional pricing models.

Therefore, our project addresses a fundamental gap: **Pure quantitative analysis is insufficient to fully explain short-term financial market behavior.** We propose a multi-modal approach that integrates traditional financial metrics with an NLP-driven sentiment coefficient to capture the "market mood."

## 2. Business Objectives
The primary business objective is to build a **Decision Support System (DSS)** for swing traders and active portfolio managers. 

Instead of building a high-frequency algorithmic trading bot, this tool is designed to assist investors in weekly portfolio rebalancing. By digesting a company's historical price momentum, its fundamental financial health, and the public sentiment surrounding it over the past week, the model provides a data-driven recommendation (Up/Down) for the stock's movement in the upcoming week ($T+1$).

## 3. Project Scope & Sample Selection
To ensure statistical rigor, computational feasibility, and high-quality data, we established strict boundaries for our dataset:

* **Target Population (110 Companies):** Instead of analyzing a single sector (which risks severe multicollinearity as stocks tend to move together) or the entire S&P 500 (which is computationally expensive for NLP tasks), we applied **Stratified Sampling**. We selected **10 top companies from each of the 11 GICS Sectors** (totaling 110 companies). This guarantees cross-sector diversity and robust statistical variance.
  A small number of tickers are manually excluded from the final universe when they create downstream data-quality problems. In particular, `PLTR` was removed because its financial history is too short for a clean 2021+ panel with the selected lagged features, while `HSBC`, `BAC`, `JPM`, and `WFC` were removed because the current feature set generates too many structural missing values for them.
* **Time Horizon (5 Years: 2021 - 2026):** We deliberately restricted our observation window to the last 5 years. This strategic decision serves two main purposes:
  1. **Excluding the "Black Swan":** It avoids the extreme, unrepeatable market volatility and anomalous patterns caused by the COVID-19 crash in early 2020.
  2. **Data Availability:** It aligns perfectly with the practical limitations of free fundamental financial APIs (e.g., `yfinance`), which reliably provide Income Statements and Balance Sheets for recent years.

## 4. Methodology: The Tri-Factor Approach
Our predictive model integrates three distinct dimensions of data:

1. **Fundamental Analysis (Intrinsic Value):** Quarterly Income Statements and Balance Sheets to evaluate the underlying financial health and leverage of the companies.
2. **Technical Analysis (Market Momentum):** Historical daily stock prices and volume to capture market trends and mean-reversion effects.
3. **Sentiment Analysis (Market Psychology):** A proxy variable for investor emotion derived from financial news. We leverage **FinBERT**, a state-of-the-art pre-trained NLP model specifically fine-tuned on financial text, to calculate daily positive/negative sentiment scores for each company.

## 5. Risks & Feasibility
* **Data Frequency Mismatch:** A core technical challenge is the integration of quarterly fundamental data with daily/weekly price and news data. We address this by treating fundamentals as "static baseline features" for a given quarter, while treating price and sentiment as "dynamic drivers."
* **Market Friction:** While the model predicts directional movement, real-world profitability is subject to transaction costs, bid-ask spreads, and latency. The model is positioned strictly as an analytical proxy rather than a fully automated execution system.

## 6. Future Work: Production Deployment & MLOps
While this academic project trains on a static 5-year historical dataset to validate the predictive power of the tri-factor model, a real-world deployment for our DSS would require a dynamic **Automated Data Pipeline**. 

To transition from a static analytical model to a live production tool, future iterations would implement the following MLOps architecture:
* **Live API Integration:** Replacing static CSV files with dynamic API calls (e.g., `yfinance` for latest weekly prices and News APIs for real-time text fetching).
* **Automated Pipeline (Cron Jobs/Airflow):** Scheduling automated scripts to run every Friday after market close. These scripts would fetch the latest week's data and run the text through our pre-trained FinBERT pipeline to generate the current sentiment proxy.
* **Dynamic Model Inference:** Feeding this live data vector into the serialized, pre-trained classification model to instantly generate the $T+1$ week prediction for the end-user dashboard.
...


# Define Data Characteristics

## How Data Is Gathered

### Company Universe
The company universe is now selected using **historical market capitalization near the start of the sample** rather than current market capitalization.

- The reference date is the first trading week of the analysis window, centered on `2021-01-04`
- The selection step combines:
  - a broad active US universe from the FMP screener
  - US companies that were later delisted after the sample start
- Each candidate is reranked using the closest historical market-cap observation around the reference date
- The final universe keeps the top 10 companies per sector based on that sample-start market cap

This choice is meant to reduce the ex-post conditioning problem that appears when firms are selected using today’s market cap and then projected backward over the sample. The rationale is consistent with the literature on **look-ahead benchmark bias**, such as Daniel, Sornette, and Woehrmann (2009), *Look-Ahead Benchmark Bias in Portfolio Performance Evaluation*.
In practical terms, this should be seen as a **strong mitigation** rather than a mathematically perfect elimination of survivorship bias, because the final quality of the historical universe still depends on the coverage and plan limits of the data provider.

### Stock Prices
Weekly prices are built from daily Yahoo Finance data.

- The script stores both `ClosePrice` and `AdjClosePrice`
- `AdjClosePrice` is kept because it adjusts for stock splits and similar corporate actions
- `ClosePrice` is also retained because it is used to update weekly market-cap-based ratios between two statement releases

The daily series is mapped to `WeekEndingFriday`, and the last available trading day of each weekly bucket is retained.

The current target variable used in the ML-ready dataset is:

`AdjClosePrice_t+1_Up = 1` if `AdjClosePrice_{t+1} > AdjClosePrice_t`, otherwise `0`


### Fundamental Data
Instead of relying on absolute accounting values such as total revenue or net income alone, the project mainly uses financial ratios and scaled variables.

Absolute values are heavily influenced by firm size. Ratios make companies more comparable and focus the analysis on profitability, growth, leverage, and balance-sheet structure, which are more relevant for cross-sectional prediction.

The final accounting feature set is intentionally compact and is designed to capture:

- profitability
- growth
- leverage
- investment
- valuation


## Selected Variables And Formulas

### Core Ratios
  BookToMarket = totalStockholdersEquity / marketCap

  GrossProfitability = grossProfit / totalAssets

  OperatingMargin = operatingIncome / revenue

  ROA = netIncome / totalAssets

  AssetGrowth = (totalAssets_t - totalAssets_{t-4q}) / totalAssets_{t-4q}

  InvestmentIntensity = |capitalExpenditure| / totalAssets

  Accruals = (netIncome - operatingCashFlow) / totalAssets

  DebtToAssets = totalDebt / totalAssets

  WorkingCapitalScaled = (totalCurrentAssets - totalCurrentLiabilities) / totalAssets

  FreeCashFlowYield = freeCashFlow / marketCap

  EarningsYield = netIncome / marketCap


### TTM Variants
TTM variants are kept in parallel for the flow-based ratios that remain stable enough for the sample:

  GrossProfitability_TTM = grossProfit_TTM / averageAssets

  OperatingMargin_TTM = operatingIncome_TTM / revenue_TTM

  ROA_TTM = netIncome_TTM / averageAssets

  Accruals_TTM = (netIncome_TTM - operatingCashFlow_TTM) / totalAssets

  FreeCashFlowYield_TTM = freeCashFlow_TTM / marketCap

  EarningsYield_TTM = netIncome_TTM / marketCap

with:

  averageAssets = (totalAssets_t + totalAssets_{t-4q}) / 2


## Financial Data Source
Financial attributes are downloaded from Financial Modeling Prep (FMP).

The current code uses the stable FMP endpoints, not the old `/api/v3/.../{ticker}` paths.

### Income Statement
Endpoint:
- `/stable/income-statement?symbol={ticker}&period=quarter&limit=60`

Fields used or retained:
- `revenue`
- `grossProfit`
- `operatingIncome`
- `netIncome`
- `interestExpense`
- `weightedAverageShsOut`
- `weightedAverageShsOutDil`

### Balance Sheet
Endpoint:
- `/stable/balance-sheet-statement?symbol={ticker}&period=quarter&limit=60`

Fields used:
- `totalAssets`
- `totalStockholdersEquity`
- `totalCurrentAssets`
- `totalCurrentLiabilities`
- `totalDebt`
- `cashAndCashEquivalents`

### Cash Flow Statement
Endpoint:
- `/stable/cash-flow-statement?symbol={ticker}&period=quarter&limit=60`

Fields used:
- `operatingCashFlow`
- `capitalExpenditure`
- `freeCashFlow`

### Market Capitalization
Endpoint:
- `/stable/enterprise-values?symbol={ticker}&period=quarter&limit=60`

Field used:
- `marketCap`


## Current Methodological Choices And Known Issues

- Quarterly accounting data are forward-filled between releases. This is preferred to interpolation because it better approximates the information set that was actually available to the market at each point in time.

- Quarterly fundamentals are aligned to the first Friday on or after the first public availability date of the statement. The preferred timestamp is `acceptedDate`, with fallback to `filingDate`. If both are missing or suspiciously earlier than the quarter-end date, the script falls back to the latest available date among `date`, `filingDate`, and `acceptedDate`. This is meant to reduce look-ahead bias.

- Weekly price data are aligned to Friday. When the last trading day of the week is not Friday because of holidays, that last observed trading day is assigned to the corresponding `WeekEndingFriday`.

- TTM versions are kept in parallel with standard quarterly ratios in order to reduce seasonality in flow variables and allow later model comparison.

- The company universe is selected using historical market capitalization at the beginning of the sample instead of today’s market capitalization. This substantially reduces survivorship and look-ahead selection bias, although a small residual bias may still remain if the provider’s historical universe coverage is incomplete.

- Market-cap-based ratios are updated weekly by taking the last quarterly market cap reported by FMP and rescaling it with weekly `ClosePrice` changes between two statement dates.

- Lagged variables are split into two groups:
  - market-based variables receive 1-week and 2-week lags
  - accounting variables receive 1-quarter and 2-quarter lags

- `InterestCoverage`, `InterestCoverage_TTM`, and all their lagged variants are excluded from the current analysis because they generate structural missing values for multiple companies and sectors.

- `ROE` and `ROE_TTM` are excluded from the current analysis. Even though the textbook formula is correct, the ratio becomes hard to interpret when book equity is negative or heavily compressed by buybacks, and in this sample it would eliminate entire companies from the complete-case ML dataset.

- `IncomeQuality`, `CashRatio`, and `InvestmentIntensity_TTM` are also excluded from the current analysis. `IncomeQuality` was too unstable when earnings were close to zero, `CashRatio` was not comparable enough across sectors, especially financials, and `InvestmentIntensity_TTM` was too exposed to provider-level scale anomalies in some companies.

- Provider-style exact zeros in `capitalExpenditure` and `freeCashFlow` are treated as missing when building ratios that use those inputs directly.

- `capitalExpenditure` is standardized to a consistent outflow sign before building `InvestmentIntensity`, so the feature remains comparable across quarters even when the provider flips the sign convention.

- `InvestmentIntensity` is stored as a positive ratio of investment spending over total assets, which makes the feature easier to interpret.

- Isolated quarters with `totalDebt = 0` between positive-debt quarters are treated as missing in the debt-based ratio because they are more likely to be provider artifacts than true debt-free states.

- `GrossProfitability_TTM` and `ROA_TTM` use average assets over one year instead of a single-quarter denominator. This is more coherent because the numerator already aggregates four quarters of flow information.

- For the final ML-ready dataset, rows containing at least one missing value are removed instead of being imputed. Given the large number of remaining observations, this complete-case approach is preferred because it avoids introducing artificial values for economically sensitive financial ratios.


## Next Steps
- Add the sentiment block to the actual pipeline, so the project becomes truly multi-modal and not only price-plus-fundamentals.
- Define a set of benchmark models, including at least a naive baseline, a price-only model, a fundamentals-only model, and the full model.
- Enrich the technical-analysis block with return-based variables such as weekly returns, momentum, and rolling volatility instead of relying mainly on price levels.
- Add an explicit preprocessing policy for modeling, including outlier treatment, scaling, and a rule that all preprocessing parameters must be estimated only on the training set.
- Validate the revised historical company-universe construction against external benchmark snapshots when possible, in order to quantify any residual survivorship bias that may still remain because of provider coverage limits.
