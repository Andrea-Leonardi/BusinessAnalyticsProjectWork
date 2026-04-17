from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import config as cfg
from splitters import split_temporal_dataframes


TARGET_COL = "AdjClosePrice_t+1_Up"
DATE_COL = "WeekEndingFriday"
SECTOR_FILTER_COLUMN = "SectorCode"
SECTOR_FILTER = 6
# Esempi:
# SECTOR_FILTER = 10          -> usa solo il settore con codice 10
# SECTOR_FILTER = [3, 10, 11] -> usa solo questi settori


def _largest_remainder_allocation(weights: pd.Series, total: int) -> pd.Series:
    if total <= 0 or weights.empty:
        return pd.Series(0, index=weights.index, dtype=int)

    normalized = weights / weights.sum()
    exact = normalized * total
    base = np.floor(exact).astype(int)
    remainder = total - int(base.sum())

    if remainder > 0:
        order = pd.DataFrame(
            {
                "fraction": exact - base,
                "weight": weights,
            },
            index=weights.index,
        ).sort_values(["fraction", "weight"], ascending=[False, False], kind="stable")
        base.loc[order.index[:remainder]] += 1

    return base.astype(int)


def _select_evenly_spaced_rows(group: pd.DataFrame, quota: int, date_col: str) -> pd.DataFrame:
    if quota <= 0:
        return group.iloc[0:0]
    if quota >= len(group):
        return group

    ordered = group.sort_values([date_col, "__original_order"], kind="stable").reset_index(drop=True)
    positions = np.floor((np.arange(quota) + 0.5) * len(ordered) / quota).astype(int)
    positions = np.clip(positions, 0, len(ordered) - 1)
    return ordered.iloc[positions]


def _balanced_majority_sample(
    majority_df: pd.DataFrame,
    total_to_keep: int,
    date_col: str,
    group_cols: list[str],
) -> pd.DataFrame:
    if total_to_keep <= 0 or majority_df.empty:
        return majority_df.iloc[0:0]
    if total_to_keep >= len(majority_df):
        return majority_df

    available = majority_df.groupby(group_cols).size().rename("available")
    n_groups = len(available)

    if total_to_keep >= n_groups:
        quotas = pd.Series(1, index=available.index, dtype=int)
        remaining = total_to_keep - n_groups
        if remaining > 0:
            extra_capacity = (available - 1).clip(lower=0)
            quotas = quotas + _largest_remainder_allocation(extra_capacity, remaining)
    else:
        quotas = _largest_remainder_allocation(available, total_to_keep)

    selected_groups = []
    for group_key, group in majority_df.groupby(group_cols, sort=False):
        quota = int(quotas.loc[group_key])
        selected_groups.append(_select_evenly_spaced_rows(group, quota, date_col))

    return pd.concat(selected_groups, axis=0, ignore_index=True)


def balance_binary_training_dataframe(
    df: pd.DataFrame,
    target_col: str,
    date_col: str,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    class_counts = df[target_col].value_counts()
    if len(class_counts) != 2 or class_counts.nunique() == 1:
        return df.copy()

    working_df = df.copy()
    working_df[date_col] = pd.to_datetime(working_df[date_col])
    working_df["__original_order"] = np.arange(len(working_df))
    working_df["__year"] = working_df[date_col].dt.year

    minority_class = class_counts.idxmin()
    majority_class = class_counts.idxmax()
    minority_df = working_df[working_df[target_col] == minority_class].copy()
    majority_df = working_df[working_df[target_col] == majority_class].copy()

    group_cols = [column for column in ["Ticker", "__year"] if column in majority_df.columns]
    if not group_cols:
        group_cols = ["__year"]

    majority_sample = _balanced_majority_sample(
        majority_df=majority_df,
        total_to_keep=len(minority_df),
        date_col=date_col,
        group_cols=group_cols,
    )

    balanced_df = pd.concat([minority_df, majority_sample], axis=0, ignore_index=True)
    balanced_df = balanced_df.sort_values([date_col, "Ticker", "__original_order"], kind="stable")
    return balanced_df.drop(columns=["__original_order", "__year"], errors="ignore").reset_index(drop=True)


def _to_xy(df: pd.DataFrame, response_var: str, exclude_vars: list[str]):
    X = df.drop(columns=[response_var] + exclude_vars, errors="ignore").reset_index(drop=True)
    y = df[response_var].reset_index(drop=True)
    return X, y


def apply_sector_filter(
    df: pd.DataFrame,
    sector_filter,
    sector_column: str,
) -> pd.DataFrame:
    if sector_filter is None:
        return df.copy()

    if sector_column not in df.columns:
        raise KeyError(
            f"Column `{sector_column}` not found in modeling dataset, "
            "so the sector filter cannot be applied."
        )

    if isinstance(sector_filter, (list, tuple, set, np.ndarray, pd.Series)):
        allowed_sectors = list(sector_filter)
    else:
        allowed_sectors = [sector_filter]

    return df[df[sector_column].isin(allowed_sectors)].copy()


def load_modeling_dataframe() -> pd.DataFrame:
    modeling_df = pd.read_csv(cfg.MODELING_DATASET).copy()
    modeling_df[DATE_COL] = pd.to_datetime(modeling_df[DATE_COL])
    modeling_df = apply_sector_filter(
        df=modeling_df,
        sector_filter=SECTOR_FILTER,
        sector_column=SECTOR_FILTER_COLUMN,
    )
    modeling_df = modeling_df.dropna().reset_index(drop=True)
    return modeling_df


def build_datasets():
    modeling_df = load_modeling_dataframe()
    train_df, validation_df, test_df = split_temporal_dataframes(modeling_df, date_col=DATE_COL)

    train_df_balanced = balance_binary_training_dataframe(train_df, TARGET_COL, DATE_COL)
    train_full_df = pd.concat([train_df, validation_df], axis=0, ignore_index=True)
    train_full_df_balanced = balance_binary_training_dataframe(train_full_df, TARGET_COL, DATE_COL)

    unused_variables = [
        DATE_COL,
        "Ticker",
        "AdjClosePrice_t+1",
    ]
    unused_variables.extend(
        column
        for column in modeling_df.columns
        if any(tag in str(column) for tag in ("EMO", "TEXTBLOB"))
    )

    X_train, y_train = _to_xy(train_df_balanced, TARGET_COL, unused_variables)
    X_validation, y_validation = _to_xy(validation_df, TARGET_COL, unused_variables)
    X_test, y_test = _to_xy(test_df, TARGET_COL, unused_variables)
    X_train_full, y_train_full = _to_xy(train_full_df_balanced, TARGET_COL, unused_variables)
    X_train_full_unbalanced, y_train_full_unbalanced = _to_xy(
        train_full_df, TARGET_COL, unused_variables
    )

    return {
        "X_train": X_train,
        "y_train": y_train,
        "X_validation": X_validation,
        "y_validation": y_validation,
        "X_test": X_test,
        "y_test": y_test,
        "X_train_full": X_train_full,
        "y_train_full": y_train_full,
        "X_train_full_unbalanced": X_train_full_unbalanced,
        "y_train_full_unbalanced": y_train_full_unbalanced,
        "modeling_df": modeling_df,
        "train_df": train_df,
        "train_df_balanced": train_df_balanced,
        "validation_df": validation_df,
        "test_df": test_df,
        "train_full_df": train_full_df,
        "train_full_df_balanced": train_full_df_balanced,
    }


DATASETS = build_datasets()

X_train = DATASETS["X_train"]
y_train = DATASETS["y_train"]
X_validation = DATASETS["X_validation"]
y_validation = DATASETS["y_validation"]
X_test = DATASETS["X_test"]
y_test = DATASETS["y_test"]
X_train_full = DATASETS["X_train_full"]
y_train_full = DATASETS["y_train_full"]
X_train_full_unbalanced = DATASETS["X_train_full_unbalanced"]
y_train_full_unbalanced = DATASETS["y_train_full_unbalanced"]
