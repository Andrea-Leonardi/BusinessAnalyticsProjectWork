**Business Understanding**


**1. Project Objectives**

1.1 Context & Problem Statement

Predicting stock movements requires combining different views of market behavior. The Efficient Market Hypothesis (EMH) treats price changes as difficult to forecast, while technical and fundamental analysis search for recurring patterns in market and company data [4]. This project focuses on the operational problem of processing price data, financial statements, and news sentiment in a single weekly modeling pipeline.

However, the need to process multi-modal data such as news stems not only from a need for speed but from a fundamental shift in our understanding of market dynamics. The classical view of the EMH, which assumes markets are populated exclusively by perfectly rational agents, has been heavily challenged by behavioral finance. Barberis and Thaler [10] highlight how "limits to arbitrage" prevent rational investors from immediately correcting price distortions. They note that the absence of profitable investment strategies does not imply the absence of mispricing: prices can be wrong for a long time, especially where trading costs discourage arbitrageurs. Financial decisions, as argued by Akerlof and Shiller [8], are deeply driven by animal spirits. Echoing Keynes, they emphasize that in the face of uncertainty about future returns, human decisions are not the result of a weighted average of quantitative probabilities, but rather a "spontaneous urge to action" driven by emotions, confidence, and collective narratives.

This emotional component has a structural impact on asset pricing. De Long et al. [7] demonstrated that noise traders (irrational investors driven by sentiment) introduce a specific risk into the market that drives prices away from their fundamental values. Surprisingly, because rational arbitrageurs cannot fully counter this risk, noise traders can even earn higher expected returns than sophisticated investors, being compensated for the risk they themselves create. Consequently, early-period investor sentiment becomes a crucial and predictive variable for understanding the cross-section of future returns.

In the digital age, these animal spirits and the actions of noise traders leave a measurable footprint. Bollen et al. [9] empirically proved that collective public mood, extracted from massive streams of online textual data (such as millions of tweets), possesses real predictive power over stock market indices like the DJIA. Their study reveals a fundamental detail: a simple one-dimensional approach (positive versus negative) is often not enough. It is by measuring mood along specific psychological dimensions (such as "Calm" or "Happiness," using multi-dimensional tools) that the accuracy of predictive models improves significantly. For this reason, the project also quantifies sentiment extracted from financial news and aligns it with the weekly stock panel.

1.2 Project Goals

The main goal of this project (StockPulse) is to predict whether each stock price will increase in the following week. Rather than using a regression approach to predict the exact price at time t+1, the task is framed as binary classification. The response variable is 0 if the price decreases in the following week and 1 if it increases. By selecting the top 10 companies for each of the 11 GICS sectors based on their market capitalization at the beginning of the period (2021), the project reduces look-ahead bias in the company universe. The modeling window starts in 2021, so the 2020 COVID-19 crash is outside the training sample.

**2. Market Context & Evolution**

2.1 Methodology Shift (1960s – 2026)

The project compares traditional machine learning models with neural network approaches and uses Financial Sentiment Analysis (FSA) to add information from news text to the tabular price and fundamental data [1].

2.2 Current Industry Standards

Recent research shows that price data alone may miss relevant information contained in fundamentals and textual signals. The project therefore focuses on two modeling priorities:

Time-Frequency Fusion: Using Fourier Transforms to find hidden cycles and seasonal patterns in price data [2].

Dynamic Relation Modeling: Moving beyond fixed industry labels to understand how different stocks actually influence each other in real-time [2].

2.3 Resources & Risk Management

Tools: We extracted all financial statements and financial information from FMP, gathered market data (such as price, adjusted close, volume, etc.) using Yahoo Finance, and integrated the Alpaca API to retrieve company news articles, from which we derived sentiment scores using FinBERT

Constraints: To handle API limits, we built a SharedRateLimiter to keep data flow consistent.

Risk Mitigation: The 2026 test data were isolated from training and validation to reduce temporal leakage.

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
This is what happened in the project. Initially, the research objective was to develop a single model for the whole market. During evaluation, however, this approach did not answer the research question well: several fitted models showed limited predictive capability, in some cases below the null model.

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

[7] De Long, J. B., Shleifer, A., Summers, L. H., & Waldmann, R. J. (1990). Noise Trader Risk in Financial Markets.* Journal of Political Economy, 98(4), 703-738.

[8] Akerlof, G. A., & Shiller, R. J. (2009). Animal Spirits: How Human Psychology Drives the Economy, and Why It Matters for Global Capitalism.* Princeton University Press.

[9] Bollen, J., Mao, H., & Zeng, X. (2011). Twitter mood predicts the stock market.* Journal of Computational Science, 2(1), 1-8.

[10] Barberis, N., & Thaler, R. (2003). A Survey of Behavioral Finance.* Handbook of the Economics of Finance, Vol. 1, Part B, 1053-1123.
