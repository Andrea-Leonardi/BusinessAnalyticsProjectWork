from pathlib import Path


# ---------------------------------------------------------------------------
# Project Paths
# ---------------------------------------------------------------------------

# Resolve the key folders used by every script in the project.
SRC = Path(__file__).resolve().parent
ROOT = SRC.parent
DATA_EXTRACTION_SRC = SRC / "dataExtraction"
MODELING_SRC = SRC / "modeling"
NEWS_EXTRACTION_SRC = SRC / "newsExtraction"
DATA = ROOT / "data"
DATA_EXTRACTION = DATA / "dataExtraction"
MODELING = DATA / "modeling"
NEWS_EXTRACTION = DATA / "newsExtraction"


# ---------------------------------------------------------------------------
# Shared Input And Output Files
# ---------------------------------------------------------------------------

# Define the common file paths used across the full analytics pipeline.
ENT = DATA_EXTRACTION / "enterprises.csv"
FMP_RAW_FINANCIALS = DATA_EXTRACTION / "financialsDataRaw.csv"
FMP_FINANCIALS = DATA_EXTRACTION / "financialsData.csv"
ALL_PRICE_DATA = DATA_EXTRACTION / "allPriceData.csv"
FULL_DATA = DATA_EXTRACTION / "fulldata.csv"
FULL_DATA_ML = DATA_EXTRACTION / "fulldata_ml.csv"
NEWS_ARTICLES = NEWS_EXTRACTION / "newsArticles.csv"
ANALYSIS_TEXT = NEWS_EXTRACTION / "textAnalysis.csv"
VECTORIZATION_BAG_OF_WORDS_FINANCIAL_PHRASEBANK = NEWS_EXTRACTION / "vectorizationBagOfWordsFinancialPhrasebank.csv"
VECTORIZATION_TFIDF_FINANCIAL_PHRASEBANK = NEWS_EXTRACTION / "vectorizationTfidfFinancialPhrasebank.csv"
VECTORIZATION_BAG_OF_WORDS_ARTICLES = NEWS_EXTRACTION / "vectorizationBagOfWordsArticles.csv"
VECTORIZATION_TFIDF_ARTICLES = NEWS_EXTRACTION / "vectorizationTfidfArticles.csv"
# ---------------------------------------------------------------------------
# Company-Level Output Folders
# ---------------------------------------------------------------------------

# Keep company-level outputs separated by data type for easier inspection.
SINGLE_COMPANY_DATA = DATA_EXTRACTION / "singleCompanyData"
SINGLE_COMPANY_PRICES = SINGLE_COMPANY_DATA / "prices"
SINGLE_COMPANY_FINANCIALS = SINGLE_COMPANY_DATA / "financials"
SINGLE_COMPANY_FULL_DATA = SINGLE_COMPANY_DATA / "fulldata"
