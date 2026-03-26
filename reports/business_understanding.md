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

## Decide How Data Will Be Gathered

## Stock Prices

- Stock prices must account for **splits and merges**.m For this reason, the variable **`Adj Close`** from `yfinance` will be used.

## Income Statement and Balance Sheet

Rather than using absolute accounting values (such as total revenue or net income), it is preferable to rely mainly on **financial ratios and growth rates**.

Absolute variables are strongly influenced by firm size and therefore introduce large scale differences across companies. For example, a large multinational will naturally have higher revenue and assets than a smaller firm, even if the latter is growing faster or performing more efficiently.

Using **ratios and variations** allows normalization across firms and focuses the analysis on **economic performance, profitability, financial structure, and growth dynamics**, which are the elements investors typically evaluate when forming expectations about future stock returns.

Another advantage of ratios and growth measures is that they capture **changes in performance**, which are often more informative for financial markets than the level itself. Stock prices tend to react to improvements or deteriorations in profitability, leverage, or growth prospects rather than to the absolute magnitude of accounting variables.

For this reason, variables such as **revenue growth, profitability margins, and leverage ratios** are commonly used in empirical asset pricing and machine learning models aimed at explaining or predicting stock returns.

Based on these considerations, a compact set of indicators derived from the **income statement** and **balance sheet** has been selected to capture four key dimensions of firm fundamentals:

**profitability, growth, leverage, and liquidity.**

---

### Selected Variables and Formulas
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

TTM variants are kept in parallel for the flow-based ratios that remain stable enough for the sample:

  GrossProfitability_TTM = grossProfit_TTM / averageAssets

  OperatingMargin_TTM = operatingIncome_TTM / revenue_TTM

  ROA_TTM = netIncome_TTM / averageAssets

  Accruals_TTM = (netIncome_TTM - operatingCashFlow_TTM) / totalAssets

  FreeCashFlowYield_TTM = freeCashFlow_TTM / marketCap

  EarningsYield_TTM = netIncome_TTM / marketCap

with:

  averageAssets = (totalAssets_t + totalAssets_{t-4q}) / 2



**Financial Attributes will be extracted from FMP**
The rationale is explained here: [deep research](reports\SelectingFinancial-StatementVariablesforWeeklyStock-ReturnPrediction.pdf)

### Income Statement
Endpoint:
- /api/v3/income-statement/{ticker}?period=quarter

Fields:
- revenue
- grossProfit
- operatingIncome
- netIncome
- interestExpense

---

### Balance Sheet
Endpoint:
- /api/v3/balance-sheet-statement/{ticker}?period=quarter

Fields:
- totalAssets
- totalStockholdersEquity
- totalCurrentAssets
- totalCurrentLiabilities
- totalDebt
- cashAndCashEquivalents

---

### Cash Flow Statement
Endpoint:
- /api/v3/cash-flow-statement/{ticker}?period=quarter

Fields:
- operatingCashFlow
- capitalExpenditure
- freeCashFlow

---
### Market Capitalization
Endpoint:
- /api/v3/historical-market-cap/{ticker}

Fields:
- marketCap

---


## Iussues
- We face the issue that financial statement data are available only at a quarterly or semi-annual frequency. To address this, we consider two alternative approaches in parallel:

  - Spline interpolation: we interpolate the data to obtain a smooth proxy of the firm’s underlying fundamentals over time. However, this approach introduces a strong assumption, since the interpolated values rely on information that is not actually available to the market at each point in time, potentially leading to look-ahead bias and reduced economic interpretability.
  - Forward-filled values: we repeat the last available observation until a new report is released. This approach better reflects the information set available to market participants. However, it may reduce the variability of the features; in particular, when including lagged variables, multiple lags may take identical values over extended periods, potentially limiting their informational content.

  We decided to go with Forward-filled to avoid the forward-looking bias

- Quarterly fundamentals are aligned to the first Friday on or after the first public availability date of the statement. The preferred timestamp is `acceptedDate`, with fallback to `filingDate`; when both are missing or suspiciously earlier than the quarter-end date, the script falls back to the latest available date among `date`, `filingDate`, and `acceptedDate`. This is meant to reduce look-ahead bias and keep the panel closer to the true market information set.

- We choose to use weekly closing prices, as they incorporate all the information accumulated during the week. Specifically, we start from daily data and select the last available price of each week.

- In some cases, due to market holidays, the last trading day may not be Friday (e.g., it could be Thursday). For consistency, we align all dates to Friday, assigning the last available price of the week to that date. This ensures a harmonized and regular weekly time index.

- We also account for seasonality in financial statement variables. For example, companies such as Apple exhibit strong seasonal patterns in revenues, with significant peaks during the holiday season.
 To mitigate this effect, we consider using **Trailing Twelve Months (TTM)** revenue, computed as the rolling sum of the last four quarters. This approach provides a smoother and more comparable measure over time and reduces the impact of seasonal fluctuations.

- al momento molte delle variabili di FMP sono fisse da un quarter all'altro e ci sta per cose che sono note al pubblico solo al'uscita delle trimestrali ma tipo il book value è un valore calcolato continuamente da prezzo dell'azione e numero di azioni quindi in realtà non è noto solo al momento delle trimestrali, questo è un problema da risolvere 
  - Update market-cap-based ratios weekly by taking the last quarterly market cap reported by FMP and rescaling it with weekly stock price changes between two statement dates.
- decido di tenere sia gli indici ttm che quelli normali e decicdere più avanti cosa includere
- inseriamo variabili finanziarie laggate, dividiamo le variabili in due tipi, quelle market based ovvero che si basano su informazioni disponibili al mercato che verranno laggate di 1 e 2 settimane mentre le variabili che si basano sui dati disponibili al rilascio della trimestrale vengono laggate di 1 e 2 trimestrali
- InterestCoverage, InterestCoverage_TTM, and all their lagged variants are excluded from the current analysis because they generate structural missing values for multiple companies and sectors.
- Provider-style exact zeros in `capitalExpenditure` and `freeCashFlow` are treated as missing when building the final ratios that use those inputs directly.
- `capitalExpenditure` is standardized to a consistent outflow sign before building `InvestmentIntensity`, so the feature remains comparable across quarters even when the raw provider flips the sign convention.
- `InvestmentIntensity` is stored as a positive ratio of investment spending over total assets. This is economically easier to interpret because higher investment now corresponds to a larger feature value instead of a more negative one.
- Isolated quarters with `totalDebt = 0` between positive-debt quarters are treated as missing in the debt-based ratio, because they are more likely to be provider artifacts than true debt-free states.
- `GrossProfitability_TTM` and `ROA_TTM` use average assets over one year instead of a single-quarter denominator. This is more coherent because the numerator already aggregates four quarters of flow information.
- `ROE` and `ROE_TTM` are excluded from the current analysis. Even though the formula is standard, the ratio becomes hard to interpret when book equity is negative or extremely compressed by share buybacks, and in our panel this would remove entire companies from the complete-case ML dataset.
- `IncomeQuality`, `CashRatio`, and `InvestmentIntensity_TTM` are also excluded from the current analysis. `IncomeQuality` was too unstable when earnings were close to zero, `CashRatio` was not comparable enough across sectors, especially financials, and `InvestmentIntensity_TTM` was too exposed to provider-level scale anomalies in some companies.
- For the final ML-ready dataset, rows containing at least one missing value are removed instead of being imputed. Given the large number of remaining observations, this complete-case approach is preferred because it keeps the modeling table easier to interpret and avoids introducing artificial values for economically sensitive financial ratios.


## Next Steps
 
