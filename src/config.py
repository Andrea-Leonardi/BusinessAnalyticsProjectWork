
from pathlib import Path


SRC = Path(__file__).resolve().parent
ROOT = SRC.parent
DATA = ROOT / "data"
ENT = DATA / "possible_enterprises" / "enterprises.csv"
SEC = DATA / "sec"
SEC_DATASET = SEC / "sec_dataset.csv"
SEC_FAILED_TICKERS = SEC / "sec_dataset_failed_tickers.csv"
SEC_COVERAGE_REPORT = SEC / "sec_coverage_report.csv"
SEC_TICKER_MAP_CACHE = SEC / "sec_company_tickers.json"
SEC_MISSING_TAGS_REPORT = SEC / "sec_missing_metric_candidates.csv"
