**Business Understanding**

**1.Determine Business Objectives**

1.1 Background & Problem Statement
In today's financial environment, stock market prediction remains a crucial yet challenging task due to the inherent volatility and uncertainty of markets [2]. Historically, this area has been a focal point of debate between the Efficient Market Hypothesis (EMH) and technical proponents. The EMH posits that stock prices follow a "random walk" and reflect all known information, while the technical proponents argue that non-random signals can be identified to obtain information about future prices [4]. 
Traditional approaches, namely Fundamental Analysis (evaluating intrinsic value) and Technical Analysis (charting price history), typically operate independently and rely heavily on manual processing [4]. This manual approach results in significant latency, making it nearly impossible to digest the massive volume of multi-modal data generated in today's digital economy.

1.2 Business Goals 
The primary objective of StockPulse is to develop an automated, end-to-end multi-modal predictive pipeline leveraging a comprehensive dataset covering the 2021–2026 period for 110 top-tier stocks. This pipeline integrates historical price action, financial fundamentals, and related news sentiment to identify the "internal laws" governing market movements. By utilizing 2021–2025 data for model optimization and reserving 2026 as a strict out-of-sample test set, we ensure rigorous temporal integrity to minimize investment risks and optimize returns [3]. We specifically transition from traditional statistical methods (e.g., ARIMA) to a Deep Learning paradigm (MLP, optimized via Optuna) to capture the complex, non-linear dynamics of financial time-series data [3].

**2. Situation Assessment & Technological Evolution**
   
2.1 Historical Evolution (1960s – 2026)
The methodology for stock prediction has evolved through four distinct phases:
1.  Technical Era (1960s-1980s): Focused on subjective manual charting and pattern recognition[4].
2.  Statistical Era (1980s-2000s): Utilized linear models like ARIMA to model historical price dependencies[3].
3.  ML Era (2000s-2015): Introduced supervised statistical classification (Random Forest, SVM) and feature engineering[4].
4.  Multi-modal Deep Learning Era (2016-2026): The current SOTA (State-of-the-Art) paradigm integrates unstructured data with numerical sequences. Financial Sentiment Analysis (FSA) has become a core requirement, allowing pipelines to automate the extraction of market psychology from global news and social sources[1].

2.2 Current SOTA Trends
Latest research in 2025-2026 indicates that processing single temporal features is no longer sufficient for high-accuracy stability[2]. Modern pipelines now prioritize:
- Time-Frequency Fusion: Integrating time-domain features with frequency-domain features (via Fourier Transform) to reveal latent periodicities and seasonality patterns in price fluctuations[2].
- Dynamic Relation Modeling: Moving away from static, pre-defined graph structures (like industry sectors) toward learnable, attention-based mapping mechanisms (e.g., NoGraphMixer) to capture evolving cross-stock dependencies[2].

2.3 Theoretical Benchmarking & SOTA Alignment
Latest research in 2025-2026 indicates that processing single temporal features is no longer sufficient for maintaining high-accuracy stability [2]. Our project aligns with these modern SOTA priorities through several key methodological links:
- Multi-modal Integration: In line with current standards, our pipeline fuses unstructured sentiment (NLP) with structural fundamentals to overcome the "information bottlenecks" found in single-source models [1, 2].
- MLP-based Structural Flexibility: Following the StockMixer philosophy, we prioritize a Deep MLP architecture. This provides the efficiency needed to capture cross-channel interactions without the inflexibility of static, pre-defined industry graphs [2, 3].
- Methodological Roadmap: While focusing on robust feature fusion, our PyTorch-based framework is future-proofed to incorporate Time-Frequency Fusion (e.g., Fourier Transforms), providing a clear path toward isolating cyclical market noise in future iterations [2, 4].

2.4 Inventory of Resources & Risks
- Resources: The project utilizes the Alpaca API for market access, Financial Modeling Prep (FMP) for fundamental data, and FinBERT/RoBERTa for NLP-driven sentiment extraction.
- Constraints: System must handle API rate limits via a synchronized “SharedRateLimiter”.
- Risks:
    - Temporal Integrity: Strict avoidance of "look-ahead bias" is required; training data must never overlap with the 2026 out-of-sample test set.
    - Data Irregularities: Handling missing entries and outliers is critical for model robustness[2].

**3. Determine Data Mining Goals**

3.1 Technical Formulation
We define the predictive task as a Binary Classification problem (Rise: 1, Non-rise: 0). Scholarly consensus suggests that a classification approach provides better predictive reliability than quantitative price regression [4]. Our technical approach implements a Deep MLP (Multi-Layer Perceptron) architecture inspired by the StockMixer philosophy, designed to capture complex inter-channel interactions between heterogeneous data streams (price action, fundamentals, and news sentiment). Instead of traditional linear models, we leverage Bayesian Hyperparameter Optimization (Optuna) to model non-linear dynamics and market momentum, ensuring robust out-of-sample performance for the 2026 test period [2, 3].

??**3.2 Success Criteria**
The primary success of the model is measured by its Predictive Accuracy on a strictly unseen 2026 Out-of-Sample (OOS) dataset. To ensure a robust evaluation, we define success through the following technical benchmarks:
- Directional Accuracy (Hit Rate):

- Sector-wise Generalization: 
  
- Model Stability (F1-Score):
  
**4. Project Plan (CRISP-DM Framework)**
Following the CRISP-DM methodology [5], the project is organized into six phases:
- Business Understanding: Defining the binary classification scope (Rise vs. Non-rise) and establishing technical success benchmarks (Accuracy > 55%, F1-Score).
- Data Understanding: Aggregating heterogeneous data streams including price action (Yahoo Finance), fundamental metrics (FMP), and non-structured news sentiment (Alpaca/FinBERT).
- Data Preparation: Implementing robust preprocessing, including missing value imputation and multi-modal feature fusion (combining technical momentum, financial ratios, and sentiment scores) [2].
- Modeling: Implementing a Deep MLP architecture optimized via Bayesian Hyperparameter Search (Optuna) to capture non-linear interactions across industry sectors.
- Evaluation: Conducting rigorous Out-of-Sample (OOS) validation on strictly unseen 2026 data, focusing on directional accuracy and model stability across diverse industry sectors [2].
- Deployment: Generating actionable weekly predictive signals (via best_model_predictions_per_company.csv) to support systematic decision-making.

**References**
[1] Du, K., et al. (2024). Financial Sentiment Analysis: Techniques and Applications.* ACM Computing Surveys, 56(9), Article 220. 
[2] Sun, W., et al. (2025). Research on deep learning model for stock prediction by integrating frequency domain and time series features.* Scientific Reports, 15:30386.
[3] Xie, Y. (2023). Stock Price Forecasting: Traditional Statistical Methods and Deep Learning Methods. Highlights in Business, Economics and Management, Vol. 21.
[4] Wikipedia (2026). Stock market prediction. [Overview of EMH, Fundamental vs. Technical methods, and Classification approaches].
[5] DataScience-PM (2026). What are the 6 CRISP-DM Phases?
