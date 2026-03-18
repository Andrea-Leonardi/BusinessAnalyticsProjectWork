
from pathlib import Path

SRC = Path(__file__).resolve().parent
ROOT = SRC.parent
DATA = ROOT / "data"
ENT = DATA / "possible_enterprises" / "enterprises.csv"
FMP = DATA / "FMP"
FMP_INCOME = FMP / "income_statements"
FMP_ALL_COMP = FMP / "income_statements_quarter_all_companies.csv"
