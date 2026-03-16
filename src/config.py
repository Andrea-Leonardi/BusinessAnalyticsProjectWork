
from pathlib import Path


ROOT = Path.cwd()
DATA = ROOT / "data"
ENT = DATA / "possible_enterprises" / "enterprises.csv"
COMPANY_NAMES_CSV = DATA / "company_names.csv"

print(ENT)