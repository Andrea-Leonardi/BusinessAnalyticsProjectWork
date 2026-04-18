from pathlib import Path
import sys

import pandas as pd


CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parent
MODELING_DIR = SRC_DIR / "4.modeling"

sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(MODELING_DIR))

import config as cfg
from splitters import split_temporal_dataframes


TARGET_COL = "AdjClosePrice_t+1_Up"
DATE_COL = "WeekEndingFriday"
SECTOR_FILTER_COLUMN = "SectorCode"


def apply_sector_filter(
    df: pd.DataFrame,
    sector_code: int,
    sector_column: str = SECTOR_FILTER_COLUMN,
) -> pd.DataFrame:
    if sector_column not in df.columns:
        raise KeyError(
            f"Column `{sector_column}` not found in modeling dataset, "
            "so the sector filter cannot be applied."
        )
    return df[df[sector_column] == sector_code].copy()


def build_feature_dataframe(test_df: pd.DataFrame) -> pd.DataFrame:
    unused_variables = [
        DATE_COL,
        "Ticker",
        "AdjClosePrice_t+1",
    ]
    unused_variables.extend(
        column
        for column in test_df.columns
        if any(tag in str(column) for tag in ("EMO", "TEXTBLOB"))
    )

    return test_df.drop(columns=[TARGET_COL] + unused_variables, errors="ignore").reset_index(drop=True)


def build_sector_test_data(sector_code: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    modeling_df = pd.read_csv(cfg.MODELING_DATASET).copy()
    modeling_df[DATE_COL] = pd.to_datetime(modeling_df[DATE_COL])
    modeling_df = apply_sector_filter(modeling_df, sector_code)
    modeling_df = modeling_df.dropna().reset_index(drop=True)

    _, _, test_df = split_temporal_dataframes(
        modeling_df,
        date_col=DATE_COL,
    )

    X_test = build_feature_dataframe(test_df)
    return test_df.reset_index(drop=True), X_test
