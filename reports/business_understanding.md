# Define Business Case

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

---

### Financial Attributes to Extract from `yfinance`

```python
ticker.financials.loc["Total Revenue"]
ticker.financials.loc["Net Income"]
ticker.financials.loc["Operating Income"]
ticker.financials.loc["EBITDA"]
ticker.financials.loc["Gross Profit"]

ticker.balance_sheet.loc["Total Assets"]
ticker.balance_sheet.loc["Stockholders Equity"]
ticker.balance_sheet.loc["Total Debt"]
ticker.balance_sheet.loc["Current Assets"]
ticker.balance_sheet.loc["Current Liabilities"]

ticker.info["marketCap"]
ticker.info["sharesOutstanding"]
```

!! YF has data only since 2023
