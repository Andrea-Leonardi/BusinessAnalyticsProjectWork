from pathlib import Path


# ---------------------------------------------------------------------------
# Project Paths
# ---------------------------------------------------------------------------

# Resolve the key folders used by every script in the project.
SRC = Path(__file__).resolve().parent
ROOT = SRC.parent
DATA = ROOT / "data"


# ---------------------------------------------------------------------------
# Shared Input And Output Files
# ---------------------------------------------------------------------------

# Define the common file paths used across the full analytics pipeline.
ENT = DATA / "enterprises.csv"
FMP_RAW_FINANCIALS = DATA / "financialsDataRaw.csv"
FMP_FINANCIALS = DATA / "financialsData.csv"
ALL_PRICE_DATA = DATA / "allPriceData.csv"


# ---------------------------------------------------------------------------
# Company-Level Output Folders
# ---------------------------------------------------------------------------

# Keep company-level outputs separated by data type for easier inspection.
SINGLE_COMPANY_DATA = DATA / "singleCompanyData"
SINGLE_COMPANY_PRICES = SINGLE_COMPANY_DATA / "prices"
SINGLE_COMPANY_FINANCIALS = SINGLE_COMPANY_DATA / "financials"
