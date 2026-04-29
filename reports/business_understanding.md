**Business Understanding**


**1. Project Objectives**

1.1 Context & Problem Statement

Predicting stock movements is a constant tug-of-war between the Efficient Market Hypothesis (EMH), which views price changes as a "random walk," and technical analysis, which seeks exploitable patterns in historical data [4]. Traditionally, fundamental and technical analyses are treated as separate silos and processed manually. This creates a significant "latency gap"—human analysts simply cannot process the massive volume of multi-modal data (news, financials, and price action) fast enough to keep up with 2026's high-frequency market environment.

1.2 Project Goals

The primary objective of StockPulse is to develop an automated, end-to-end multi-modal predictive pipeline for 110 leading stocks, ensuring that our model learns universal market dynamics rather than being skewed toward a specific industry like Technology or Finance. Our dataset focuses on the 2021–2026 period, deliberately starting after 2020 to exclude "Black Swan" anomalies. By filtering out the extreme, non-recurring volatility caused by the 2020 COVID-19 crash, we ensure the model learns from modern market regimes rather than anomalous patterns. This pipeline integrates historical price action, financial fundamentals, and news sentiment to identify the "internal laws" governing market movements. By utilizing 2021–2025 data for model optimization and reserving 2026 as a test set, we ensure rigorous temporal integrity to minimize investment risks [3].

**2. Market Context & Evolution**

2.1 Methodology Shift (1960s – 2026)

We have moved past the eras of manual charting (1960s) and basic linear statistics like ARIMA (1980s). While machine learning (Random Forest, SVM) improved results after 2000, the 2026 standard is Multi-Modal Deep Learning. Today, a competitive model must automate Financial Sentiment Analysis (FSA) to capture market psychology directly from news feeds [1].

2.2 Current Industry Standards

Leading research in 2025-2026 shows that simple price tracking is no longer enough. Our project focuses on two modern priorities:

Time-Frequency Fusion: Using Fourier Transforms to find hidden cycles and seasonal patterns in price data [2].

Dynamic Relation Modeling: Moving beyond fixed industry labels to understand how different stocks actually influence each other in real-time [2].

2.3 Resources & Risk Management

Tools: We integrated the Alpaca API for market data, FMP for fundamentals, and FinBERT for sentiment scoring.

Constraints: To handle API limits, we built a SharedRateLimiter to keep data flow consistent.

Risk Mitigation: Our primary focus is on maintaining temporal integrity. The 2026 test data were strictly isolated to ensure the model doesn't "peek" into the future during training.

**3. Data Mining Goals**

3.1 Technical Strategy

We are treating price prediction as a binary classification task (Rise: 1, Non-rise: 0). We chose this over price regression because classification offers more reliable signals for actual trading decisions [4]. Our architecture, inspired by StockMixer, uses a Deep MLP to fuse different data streams. We use Optuna (Bayesian Optimization) to fine-tune the model, ensuring it adapts to market momentum rather than just memorizing historical noise [2, 3].

3.2 Success Criteria

The model’s value is judged by its performance on the unseen 2026 dataset:


Hit Rate (Accuracy): We target a consistent directional accuracy above 57% across key sectors [6].

Sector Specialization: Rather than a "one-size-fits-all" approach, we measure success by how well the model adapts to 11 independent industry groups. Our results show it is highly effective in growth sectors (Tech, Healthcare, Industrials) while identifying the limits of sentiment-based prediction in commodity-heavy sectors like Energy.

Stability (F1-Score): We use the F1-Score to ensure the model isn't just "guessing" the majority trend but is accurately identifying both upward and downward signals.

**4. Project Plan (CRISP-DM)**

We follow the CRISP-DM framework across six clear stages:

Business Understanding: Setting the classification goals and the 57% accuracy benchmark.

Data Understanding: Gathering price, fundamental, and FinBERT-processed sentiment data.

Data Preparation: Cleaning missing data and merging heterogeneous features into a single pipeline [2].

Modeling: Building and optimizing the Deep MLP via Optuna.

Evaluation: Running a strict 2026 out-of-sample test to check for real-world stability [2].

Deployment: Exporting weekly predictions (best_model_predictions_per_company.csv) to support systematic trading.


References

[1] Du, K., et al. (2024). Financial Sentiment Analysis: Techniques and Applications.* ACM Computing Surveys, 56(9), Article 220. 

[2] Sun, W., et al. (2025). Research on deep learning model for stock prediction by integrating frequency domain and time series features.* Scientific Reports, 15:30386.

[3] Xie, Y. (2023). Stock Price Forecasting: Traditional Statistical Methods and Deep Learning Methods. Highlights in Business, Economics and Management, Vol. 21.

[4] Wikipedia (2026). Stock market prediction. [Overview of EMH, Fundamental vs. Technical methods, and Classification approaches].

[5] DataScience-PM (2026). What are the 6 CRISP-DM Phases?

[6] Weinberg.A.I (2025). Hybrid Quantum-Classical Ensemble Learning for S&P 500 Directional Prediction
