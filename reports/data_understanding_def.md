**Business Understanding**


**1. Project Objectives**

1.1 Context & Problem Statement

Predicting stock movements is a constant tug-of-war between the Efficient Market Hypothesis (EMH), which views price changes as a "random walk," and technical analysis, which seeks exploitable patterns in historical data [4]. Traditionally, fundamental and technical analyses are treated as separate silos and processed manually. This creates a significant "latency gap"—human analysts simply cannot process the massive volume of multi-modal data (news, financials, and price action) fast enough to keep up with 2026's high-frequency market environment.

1.2 Project Goals

The main goal of this project (StockPulse) is to help investors make better decisions faster and more accurately. Rather than using a regression approach to predict the exact price at time t+1 (using weekly frequency data)—which often introduces excessive noise—we framed the task as a binary classification problem. We used a binary response variable that takes the value of 0 if the price decreases in the following week, and 1 if it increases. Therefore, our goal is simply to predict 0 or 1 to provide a reliable edge for weekly capital allocation. By selecting the top 10 companies for each of the 11 GICS sectors based on their market capitalization at the beginning of the period (2021), we ensure our analysis avoids survivorship bias and that our insights are driven by universal market laws rather than industry-specific bubbles. To safeguard against unrepeatable volatility, we deliberately focused our modeling on the 2021–2026 period, filtering out the 2020 COVID-19 "Black Swan" to align with modern market regimes.

**2. Market Context & Evolution**

2.1 Methodology Shift (1960s – 2026)

We have moved past the eras of manual charting (1960s) and basic linear statistics like ARIMA (1980s). While machine learning (Random Forest, SVM) improved results after 2000, the 2026 standard is Multi-Modal Deep Learning. Today, a competitive model must automate Financial Sentiment Analysis (FSA) to capture market psychology directly from news feeds [1].

2.2 Current Industry Standards

Leading research in 2025-2026 shows that simple price tracking is no longer enough. Our project focuses on two modern priorities:

Time-Frequency Fusion: Using Fourier Transforms to find hidden cycles and seasonal patterns in price data [2].

Dynamic Relation Modeling: Moving beyond fixed industry labels to understand how different stocks actually influence each other in real-time [2].

2.3 Resources & Risk Management

Tools: We extracted all financial statements and financial information from FMP, gathered market data (such as price, adjusted close, volume, etc.) using Yahoo Finance, and integrated the Alpaca API to retrieve company news articles, from which we derived sentiment scores using FinBERT

Constraints: To handle API limits, we built a SharedRateLimiter to keep data flow consistent.

Risk Mitigation: Our primary focus is on maintaining temporal integrity. The 2026 test data were strictly isolated to ensure the model doesn't "peek" into the future during training.

**3. Data Mining Goals**

3.1 Technical Strategy

We are treating price prediction as a binary classification task (Rise: 1, Non-rise: 0). We chose this over price regression because classification offers more reliable signals for actual trading decisions [4]. We adopted a comprehensive modeling strategy by first training several classic machine learning models (a more traditional approach). Alongside these, our architecture, inspired by StockMixer, uses a Deep MLP to combine different data streams, utilizing Optuna (Bayesian Optimization) to fine-tune the model so it adapts to market trends rather than simply memorizing historical data [2, 3]. Ultimately, for each individual sector, we fitted all these different models and selected the one with the best predictive capacity, meaning the highest accuracy.

3.2 Success Criteria

The model’s value is judged by its performance on the unseen 2026 dataset:


Hit Rate (Accuracy): Ultimately, business success is not defined by a fixed percentage threshold (e.g., 57%) as seen in other academic papers [6]. Instead, since we fitted a different model for each sector, we evaluate success by comparing our models against a null model (which always predicts the majority class). A model is considered successful if it achieves a higher accuracy than the null model in both the training set (fitted on data from 2021 to 2025) and the test set (representing 2026 data), accounting for the changing proportions of 0s and 1s in each set, while maintaining rigorous temporal integrity to minimize real-world investment risks [3].

Sector Specialization: Rather than a "one-size-fits-all" approach, we measure success by the selected models' ability to accurately predict the outcome of interest (0 or 1) for the companies within each specific industry group. Our results show it is highly effective in growth sectors (Tech, Healthcare, Industrials) while identifying the limits of sentiment-based prediction in commodity-heavy sectors like Energy.

Stability (F1-Score): We use the F1-Score to ensure the model isn't just "guessing" the majority trend but is accurately identifying both upward and downward signals.

**4. Project Plan (CRISP-DM)**

We follow the CRISP-DM framework across six clear stages:

Business Understanding: Setting the classification goals and the 57% accuracy benchmark.

Data Understanding: Gathering price, fundamental, and FinBERT-processed sentiment data.

Data Preparation: Cleaning missing data and merging heterogeneous features into a single pipeline [2].

Modeling: Building and optimizing the Deep MLP via Optuna.

Evaluation: Running a strict 2026 out-of-sample test to check for real-world stability [2].

Deployment: Exporting weekly predictions (best_model_predictions_per_company.csv) to support systematic trading.

4.1 The Iterative Process of CRISP-DM

One of the main characteristics of CRISP-DM, which distinguishes it from other cycles considered best practices for successful analysis, is its iterative nature and the presence of arrows indicating bidirectional flows. Consequently, it is not mandatory to always move forward; as can be easily understood, in many contexts it is not appropriate to proceed to the next activity when the results obtained from previous phases are poor and unreliable. The framework emphasizes that one can, and indeed must, go back when deemed necessary.
This is exactly what happened in our case. Initially, our research objective was to develop a single universal model to predict the entire stock market. However, once we reached the penultimate phase, namely Evaluation, we realized that this approach did not satisfy our research question: all the fitted models showed very limited predictive capabilities, in several cases even worse than the null model, and some were almost comparable to a coin flip.

To address this, we initially decided to conduct a more accurate exploratory analysis, to understand if we had missed something important for predicting the potential increase or decrease in price the following week. Soon, however, we realized that the issue was not due to forgetting variables, selecting the wrong features, or making other mistakes. We tried to put ourselves in the shoes of a machine learning model, to try to understand, based on its optimization criteria, how it could discriminate between observations with a "down" label and those with an "up" label. We realized that, although the human brain can reason and identify patterns at a much higher level than a simple computer model, not even we were able to find a justification for why the price went up in one case and down in another.

We then applied macroeconomic reasoning to understand the performance of the sectors in relation to potential macro events that would influence them simultaneously. Almost immediately, we realized that our research question was probably too ambitious. It was very difficult to fit a model capable of predicting the market as a whole, without any form of distinction between sectors, given that the market does not move in unison. Simply put, macroeconomic laws show us that when central banks raise interest rates, the financial services sector tends to grow, driven mainly by banks that have the ability to lend at higher rates and obtain higher returns from their deposits at the central bank. On the other hand, since investments and the purchase of new properties by families are discouraged due to high interest rates, the Real Estate sector tends to decline and experience negative trends.

As we had defined the model, it was unable to distinguish companies based on their sector. For this reason, in order to help the model achieve greater predictive capacity, reduce the noise present in the data, and facilitate the identification of patterns (which became particularly complex when considering companies from different industries), we decided to reshape our research question. Rather than identifying a single model to predict the stock market as a whole, we aimed to develop models capable of anticipating upward or downward price movements separately for individual sectors.

We therefore conclude by stating that, from the Evaluation phase, we returned to Business Understanding to repeat all the processes from the beginning. In particular, we evaluated whether the work done up to that point was adequate for the new objective and, where necessary, we made corrections.

References

[1] Du, K., et al. (2024). Financial Sentiment Analysis: Techniques and Applications.* ACM Computing Surveys, 56(9), Article 220. 

[2] Sun, W., et al. (2025). Research on deep learning model for stock prediction by integrating frequency domain and time series features.* Scientific Reports, 15:30386.

[3] Xie, Y. (2023). Stock Price Forecasting: Traditional Statistical Methods and Deep Learning Methods. Highlights in Business, Economics and Management, Vol. 21.

[4] Wikipedia (2026). Stock market prediction. [Overview of EMH, Fundamental vs. Technical methods, and Classification approaches].

[5] DataScience-PM (2026). What are the 6 CRISP-DM Phases?

[6] Weinberg.A.I (2025). Hybrid Quantum-Classical Ensemble Learning for S&P 500 Directional Prediction
