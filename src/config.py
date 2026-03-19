
from pathlib import Path

SRC = Path(__file__).resolve().parent
ROOT = SRC.parent
DATA = ROOT / "data"
ENT = DATA / "enterprises.csv"
FMP_FINANCIALS = DATA / "financialsData.csv"
ALL_PRICE_DATA = DATA / "allPriceData.csv"
SINGLE_COMPANY_DATA = DATA / "singleCompanyData"
