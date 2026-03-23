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

- **Revenue Growth**  
  `Revenue_growth = (Revenue_t − Revenue_{t-1}) / Revenue_{t-1}`  
  Measures how fast company sales are expanding.

- **Earnings Growth**  
  `Earnings_growth = (NetIncome_t − NetIncome_{t-1}) / NetIncome_{t-1}`  
  Captures improvements or deteriorations in profitability.

- **Net Profit Margin**  
  `Net_margin = NetIncome / Revenue`  
  Indicates how much profit is generated for each unit of revenue.

- **Operating Margin**  
  `Operating_margin = OperatingIncome / Revenue`  
  Measures operational efficiency excluding non-operating effects.

- **EBITDA Margin**  
  `EBITDA_margin = EBITDA / Revenue`  
  Provides a profitability measure independent of capital structure and depreciation.

- **Return on Equity (ROE)**  
  `ROE = NetIncome / ShareholdersEquity`  
  Evaluates how efficiently the firm generates profits from shareholder capital.

- **Return on Assets (ROA)**  
  `ROA = NetIncome / TotalAssets`  
  Measures how effectively company assets are used to produce earnings.

- **Debt-to-Equity Ratio**  
  `Debt_to_equity = TotalDebt / ShareholdersEquity`  
  Indicates financial leverage and risk exposure.

- **Debt-to-Assets Ratio**  
  `Debt_to_assets = TotalDebt / TotalAssets`  
  Captures the proportion of assets financed through debt.

- **Current Ratio**  
  `Current_ratio = CurrentAssets / CurrentLiabilities`  
  Measures short-term liquidity and the ability to meet near-term obligations.

These indicators summarize the most relevant aspects of firm fundamentals while remaining relatively compact and comparable across companies. Combined with **sentiment indicators extracted from news data**, they allow the model to incorporate both **market perception (textual sentiment)** and **economic fundamentals**, which together may help explain and predict future stock price movements.

Financial Attributes will be extracted from FMP


## Iussues
- We face the issue that financial statement data are available only at a quarterly or semi-annual frequency. To address this, we consider two alternative approaches in parallel:

  - Spline interpolation: we interpolate the data to obtain a smooth proxy of the firm’s underlying fundamentals over time. However, this approach introduces a strong assumption, since the interpolated values rely on information that is not actually available to the market at each point in time, potentially leading to look-ahead bias and reduced economic interpretability.
  - Forward-filled values: we repeat the last available observation until a new report is released. This approach better reflects the information set available to market participants. However, it may reduce the variability of the features; in particular, when including lagged variables, multiple lags may take identical values over extended periods, potentially limiting their informational content.

  We will empirically compare the two approaches to evaluate the trade-off between realism and smoothness.

- We choose to use weekly closing prices, as they incorporate all the information accumulated during the week. Specifically, we start from daily data and select the last available price of each week.

- In some cases, due to market holidays, the last trading day may not be Friday (e.g., it could be Thursday). For consistency, we align all dates to Friday, assigning the last available price of the week to that date. This ensures a harmonized and regular weekly time index.

- We also account for seasonality in financial statement variables. For example, companies such as Apple exhibit strong seasonal patterns in revenues, with significant peaks during the holiday season.
 To mitigate this effect, we consider using **Trailing Twelve Months (TTM)** revenue, computed as the rolling sum of the last four quarters. This approach provides a smoother and more comparable measure over time and reduces the impact of seasonal fluctuations.

- The cubic spline becomes unstable after the last available quarterly observation. Therefore, the dataset must be truncated to avoid unreliable extrapolations.
  
## Next Steps

- Fix date alignment issues related to spline interpolation  
- Select the financial indicators to include and update the data extraction code accordingly  
- Add lagged financial variables  
- Address seasonality issues in financial data  
- Develop a pipeline to merge all features for each company, including a placeholder for sentiment variables