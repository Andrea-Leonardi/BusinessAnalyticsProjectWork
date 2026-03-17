
from pathlib import Path


SRC = Path(__file__).resolve().parent
ROOT = SRC.parent
DATA = ROOT / "data"
ENT = DATA / "possible_enterprises" / "enterprises.csv"
SEC_DATASET = DATA / "sec_dataset.csv"
SEC_FAILED_TICKERS = DATA / "sec_dataset_failed_tickers.csv"
