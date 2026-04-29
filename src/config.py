from pathlib import Path


# ---------------------------------------------------------------------------
# Project Paths
# ---------------------------------------------------------------------------

# Root del modulo shared config e root del repository.
SRC = Path(__file__).resolve().parent
PROJECT_ROOT = SRC.parent
ROOT = PROJECT_ROOT

# Sorgenti principali del progetto dopo il riordino in cartelle numerate.
DATA_EXTRACTION_ROOT = SRC / "1.dataExtraction"
NEWS_EXTRACTION_ROOT = SRC / "2.newsExtraction"
MODELING_NEWS_ROOT = SRC / "3.modeling_news"
MODELING_ROOT = SRC / "4.modeling"
RELATIONAL_DATABASE_ROOT = SRC / "5.relationalDatabase"
EVALUATION_ROOT = SRC / "6.evaluation"

# Alias compatibili con il codice esistente.
# Nota: i path *_SRC storicamente vengono usati per lanciare gli script di pipeline,
# quindi qui puntano direttamente alla sottocartella "pipeline".
DATA_EXTRACTION_SRC = DATA_EXTRACTION_ROOT / "pipeline"
NEWS_EXTRACTION_SRC = NEWS_EXTRACTION_ROOT / "pipeline"
MODELING_SRC = MODELING_ROOT
RELATIONAL_DATABASE_SRC = RELATIONAL_DATABASE_ROOT


# ---------------------------------------------------------------------------
# Data Paths
# ---------------------------------------------------------------------------

DATA = PROJECT_ROOT / "data"
DATA_EXTRACTION = DATA / "dataExtraction"
NEWS_EXTRACTION = DATA / "newsExtraction"
MODELING = DATA / "modeling"
HF_CACHE = DATA / "hf_cache"

REPORTS = PROJECT_ROOT / "reports"
NOTEBOOKS = PROJECT_ROOT / "notebooks"


# ---------------------------------------------------------------------------
# Pipeline Scripts
# ---------------------------------------------------------------------------

DATA_EXTRACTION_RUNNER = DATA_EXTRACTION_ROOT / "rundataExtraction.py"
NEWS_EXTRACTION_RUNNER = NEWS_EXTRACTION_ROOT / "runFullPipeline.py"

DATA_EXTRACTION_COMPANY_SELECTION_SCRIPT = DATA_EXTRACTION_SRC / "1.FMP_companySelection.py"
DATA_EXTRACTION_PRICE_SCRIPT = DATA_EXTRACTION_SRC / "2.priceDataGathering.py"
DATA_EXTRACTION_FINANCIALS_GATHERING_SCRIPT = (
    DATA_EXTRACTION_SRC / "3.FMP_financialsDataGathering.py"
)
DATA_EXTRACTION_FINANCIALS_PROCESSING_SCRIPT = (
    DATA_EXTRACTION_SRC / "4.FMP_financialsDataProcessing.py"
)
DATA_EXTRACTION_MERGE_SCRIPT = DATA_EXTRACTION_SRC / "5.FMP_dataMerge.py"

NEWS_DATA_GATHERING_SCRIPT = NEWS_EXTRACTION_SRC / "1.newsDataGathering.py"
NEWS_MISSING_SUMMARY_SCRIPT = NEWS_EXTRACTION_SRC / "2.missingSummaryImputation.py"
NEWS_MAINTENANCE_SCRIPT = NEWS_EXTRACTION_SRC / "3.newsMaintenance.py"
NEWS_TEXT_ANALYSIS_SCRIPT = NEWS_EXTRACTION_SRC / "4.textAnalysis.py"
NEWS_WEEKLY_AGGREGATION_SCRIPT = NEWS_EXTRACTION_SRC / "5.weeklyNewsAggregation.py"


# ---------------------------------------------------------------------------
# Shared Input And Output Files
# ---------------------------------------------------------------------------

ENT = DATA_EXTRACTION / "enterprises.csv"
COMPANY_SELECTION_UNIVERSE = DATA_EXTRACTION / "companySelectionUniverse.csv"
FMP_RAW_FINANCIALS = DATA_EXTRACTION / "financialsDataRaw.csv"
FMP_FINANCIALS = DATA_EXTRACTION / "financialsData.csv"
ALL_PRICE_DATA = DATA_EXTRACTION / "allPriceData.csv"
FULL_DATA = DATA_EXTRACTION / "fulldata.csv"
FULL_DATA_ML = DATA_EXTRACTION / "fulldata_ml.csv"
FULL_DATA_WITH_NEWS = DATA_EXTRACTION / "fulldata_with_news.csv"

NEWS_ARTICLES = NEWS_EXTRACTION / "newsArticles.csv"
ANALYSIS_TEXT = NEWS_EXTRACTION / "textAnalysis.csv"
ANALYSIS_TEXT_CACHE = NEWS_EXTRACTION / "textAnalysisCache.csv"
ANALYSIS_TEXT_WEEKLY = NEWS_EXTRACTION / "textAnalysisWeekly.csv"
TRAINING_ARTICLES = NEWS_EXTRACTION / "trainingArticles.csv"
DATA_GRANGER = NEWS_EXTRACTION / "dataGranger.csv"
GRANGER_LAG1_LAG2 = NEWS_EXTRACTION / "granger_coeffs_pvalues.csv"
GRANGER_FINBERT_COEFFICIENTS = NEWS_EXTRACTION / "granger_finbert_coefficients.csv"
RAW_NEWS_DATA = NEWS_EXTRACTION / "raw_news_data"
HF_FINANCIAL_PHRASEBANK_LOCAL_DIR = HF_CACHE / "financial_phrasebank"
HF_FINANCIAL_PHRASEBANK_DATASET_FILENAME = "data/FinancialPhraseBank-v1.0.zip"

VECTORIZATION_BAG_OF_WORDS_FINANCIAL_PHRASEBANK = (
    NEWS_EXTRACTION / "vectorizationBagOfWordsFinancialPhrasebank.csv"
)
VECTORIZATION_TFIDF_FINANCIAL_PHRASEBANK = (
    NEWS_EXTRACTION / "vectorizationTfidfFinancialPhrasebank.csv"
)
VECTORIZATION_BAG_OF_WORDS_ARTICLES = (
    NEWS_EXTRACTION / "vectorizationBagOfWordsArticles.csv"
)
VECTORIZATION_TFIDF_ARTICLES = NEWS_EXTRACTION / "vectorizationTfidfArticles.csv"

MODELING_DATASET = MODELING / "modeling.csv"
RELATIONAL_DATABASE_VARIABLES_CATALOG = (
    RELATIONAL_DATABASE_ROOT / "metadata.csv"
)


# ---------------------------------------------------------------------------
# Evaluation Outputs
# ---------------------------------------------------------------------------

EVALUATION_BASIC_MATERIALS = (
    EVALUATION_ROOT / "1.Basic Materials" / "best_model_predictions_per_company.csv"
)
EVALUATION_COMMUNICATION_SERVICES = (
    EVALUATION_ROOT / "2.Communication Services" / "best_model_predictions_per_company.csv"
)
EVALUATION_CONSUMER_CYCLICAL = (
    EVALUATION_ROOT / "3.Consumer Cyclical" / "best_model_predictions_per_company.csv"
)
EVALUATION_CONSUMER_DEFENSIVE = (
    EVALUATION_ROOT / "4.Consumer Defensive" / "best_model_predictions_per_company.csv"
)
EVALUATION_ENERGY = (
    EVALUATION_ROOT / "5.Energy" / "best_model_predictions_per_company.csv"
)
EVALUATION_FINANCIAL_SERVICES = (
    EVALUATION_ROOT / "6.Financial Services" / "best_model_predictions_per_company.csv"
)
EVALUATION_HEALTHCARE = (
    EVALUATION_ROOT / "7.Healthcare" / "best_model_predictions_per_company.csv"
)
EVALUATION_INDUSTRIALS = (
    EVALUATION_ROOT / "8.Industrials" / "best_model_predictions_per_company.csv"
)
EVALUATION_REAL_ESTATE = (
    EVALUATION_ROOT / "9.Real Estate" / "best_model_predictions_per_company.csv"
)
EVALUATION_TECHNOLOGY = (
    EVALUATION_ROOT / "10.Technology" / "best_model_predictions_per_company.csv"
)
EVALUATION_UTILITIES = (
    EVALUATION_ROOT / "11.Utilities" / "best_model_predictions_per_company.csv"
)

EVALUATION_PREDICTIONS_BY_FOLDER = {
    "1.Basic Materials": EVALUATION_BASIC_MATERIALS,
    "2.Communication Services": EVALUATION_COMMUNICATION_SERVICES,
    "3.Consumer Cyclical": EVALUATION_CONSUMER_CYCLICAL,
    "4.Consumer Defensive": EVALUATION_CONSUMER_DEFENSIVE,
    "5.Energy": EVALUATION_ENERGY,
    "6.Financial Services": EVALUATION_FINANCIAL_SERVICES,
    "7.Healthcare": EVALUATION_HEALTHCARE,
    "8.Industrials": EVALUATION_INDUSTRIALS,
    "9.Real Estate": EVALUATION_REAL_ESTATE,
    "10.Technology": EVALUATION_TECHNOLOGY,
    "11.Utilities": EVALUATION_UTILITIES,
}

EVALUATION_PREDICTIONS_BY_SECTOR = {
    "Basic Materials": EVALUATION_BASIC_MATERIALS,
    "Communication Services": EVALUATION_COMMUNICATION_SERVICES,
    "Consumer Cyclical": EVALUATION_CONSUMER_CYCLICAL,
    "Consumer Defensive": EVALUATION_CONSUMER_DEFENSIVE,
    "Energy": EVALUATION_ENERGY,
    "Financial Services": EVALUATION_FINANCIAL_SERVICES,
    "Healthcare": EVALUATION_HEALTHCARE,
    "Industrials": EVALUATION_INDUSTRIALS,
    "Real Estate": EVALUATION_REAL_ESTATE,
    "Technology": EVALUATION_TECHNOLOGY,
    "Utilities": EVALUATION_UTILITIES,
}

EVALUATION_BEST_MODELS_SUMMARY = EVALUATION_ROOT / "best_models_summary.csv"


# ---------------------------------------------------------------------------
# Company-Level Output Folders
# ---------------------------------------------------------------------------

SINGLE_COMPANY_DATA = DATA_EXTRACTION / "singleCompanyData"
SINGLE_COMPANY_PRICES = SINGLE_COMPANY_DATA / "prices"
SINGLE_COMPANY_FINANCIALS = SINGLE_COMPANY_DATA / "financials"
SINGLE_COMPANY_FULL_DATA = SINGLE_COMPANY_DATA / "fulldata"


# ---------------------------------------------------------------------------
# Source Asset Folders
# ---------------------------------------------------------------------------

MODELING_NEWS_BEST_PARAMS = MODELING_NEWS_ROOT / "bestParams"
MODELING_NEWS_GRAPHS = MODELING_NEWS_ROOT / "graphs"
