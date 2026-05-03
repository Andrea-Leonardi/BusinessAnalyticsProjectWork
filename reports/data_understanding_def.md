**Data Understanding**


For this project, we built a high-dimensional dataset covering 110 stocks across 11 GICS sectors, spanning from 2021 to 2026.

**1. Data Collection & Universe Selection**

We pulled raw data from three main sources to capture a complete view of market behavior:

**Universe Selection** (11x10 Framework): To ensure a balanced sample, we selected the top 10 companies by market capitalization from each of the 11 GICS sectors. This 110-stock universe was ranked using historical market cap as of January 4, 2021, ensuring we selected firms that were industry leaders at the start of the study to avoid "look-ahead bias."

**Price Data:** We used the Alpaca API and Yahoo Finance to collect weekly OHLCV sequences. By starting in 2021, we intentionally excluded the 2020 "Black Swan" COVID-19 crash to focus on modern market regimes.

**Financial Fundamentals:** Quarterly and annual ratios were extracted from Financial Modeling Prep (FMP). We included delisted companies in our initial screen to mitigate survivorship bias, ensuring the model reflects a realistic historical environment rather than just "today's winners."

**Sentiment Data:** Unstructured news headlines were processed through FinBERT and RoBERTa pipelines. These daily signals were time-aggregated into weekly vectors to capture the prevailing market mood for each ticker.

**2. Data Description**

The final dataset is structured as a multi-channel time series, synchronized by ticker and "Week-Ending Friday" timestamps.


**Key Variables:** Features include technical indicators (moving averages, momentum), financial ratios (P/E, revenue growth, debt-to-equity), and sentiment vectors (polarity and news volume).

**Feature Alignment:** We used a "point-in-time" approach for fundamental data, ensuring quarterly metrics are only visible to the model after their actual release dates to prevent data leakage.

**Dataset Scale: **The data covers roughly five years. We used 2021–2025 for model training and hyperparameter tuning, while 2026 data was strictly reserved as a test dataset.


**3. Data Exploration**

Our initial analysis highlighted significant differences in how various industries behave:

**Sector Volatility:** Growth sectors like Technology and Healthcare showed a much stronger correlation between news sentiment and price movement than defensive or commodity-driven sectors.

**Predictive Patterns:** As shown in Figure 1, "Rise/Non-rise" signals are more distinct in innovation-driven industries.

**Metric Selection:** Because market shocks often cause an imbalance in price direction, relying solely on Accuracy can be misleading. We introduced the F1-Score to measure the model's ability to balance precision (avoiding false signals) and recall (capturing actual growth).


Regarding the 2026 test results, Healthcare and Industrials performed best, with F1 scores above 0.67, proving the model is highly reliable in these areas. Conversely, lower scores in Energy and Basic Materials suggest the framework hits its limits when faced with the extreme volatility of commodity markets.

(Figure 1)

**4. Data Quality Control**

To ensure the 2026 predictions were grounded in reality, we addressed several technical challenges:


**Missing Data:** We found gaps in news coverage for some smaller stocks and used forward-fill imputation to maintain the time-series flow without "inventing" data.

**Handling Outliers:** We flagged extreme moves like flash crashes. These were kept to test model resilience but were stabilized using Z-score scaling to prevent them from skewing the neural network's weights.

**API Limiter:** To stay within Alpaca and FMP request limits, we implemented a SharedRateLimiter to prevent data fragmentation.

**Temporal Integrity:** We enforced a strict "firewall" between the 2021–2025 development phase and the 2026 test set. This total isolation is critical to ensuring our 57%+ accuracy is a true reflection of real-world performance.
