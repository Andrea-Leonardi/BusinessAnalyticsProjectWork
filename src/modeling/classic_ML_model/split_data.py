from pathlib import Path
import sys

from pandas import read_csv


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import config as cfg
from splitters import split_data_by_date



X_train, y_train, X_validation, y_validation, X_test, y_test = split_data_by_date(cfg.FULL_DATA, "AdjClosePrice_t+1_Up", ["WeekEndingFriday","Ticker"], "WeekEndingFriday")

