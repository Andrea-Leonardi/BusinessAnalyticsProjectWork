from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import config as cfg
from splitters import split_data_by_date

import numpy as np
import pandas as pd


TARGET_COL = "AdjClosePrice_t+1_Up"
DATE_COL = "WeekEndingFriday"


def _largest_remainder_allocation(weights: pd.Series, total: int) -> pd.Series:
    """Allocate an integer quota proportionally and deterministically."""
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
    """Pick representative rows spread across time instead of random rows."""
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


def balance_binary_target_by_time(
    df: pd.DataFrame,
    target_col: str,
    date_col: str,
    split_years: dict[str, list[int]],
) -> pd.DataFrame:
    """Balance each temporal split while preserving ticker-year coverage."""
    balanced_parts = []

    for years in split_years.values():
        split_df = df[df[date_col].dt.year.isin(years)].copy()
        if split_df.empty:
            continue

        class_counts = split_df[target_col].value_counts()
        if len(class_counts) != 2 or class_counts.nunique() == 1:
            balanced_parts.append(split_df)
            continue

        minority_class = class_counts.idxmin()
        majority_class = class_counts.idxmax()
        minority_df = split_df[split_df[target_col] == minority_class].copy()
        majority_df = split_df[split_df[target_col] == majority_class].copy()

        majority_sample = _balanced_majority_sample(
            majority_df=majority_df,
            total_to_keep=len(minority_df),
            date_col=date_col,
            group_cols=["Ticker", "__year"],
        )

        balanced_parts.append(pd.concat([minority_df, majority_sample], axis=0, ignore_index=True))

    balanced_df = pd.concat(balanced_parts, axis=0, ignore_index=True)
    balanced_df = balanced_df.sort_values([date_col, "Ticker", "__original_order"], kind="stable")
    return balanced_df.drop(columns=["__original_order", "__year"], errors="ignore").reset_index(drop=True)


modeling_df = pd.read_csv(cfg.MODELING_DATASET).copy()
parsed_dates = pd.to_datetime(modeling_df[DATE_COL])
modeling_df[DATE_COL] = parsed_dates
modeling_df["__original_order"] = np.arange(len(modeling_df))
modeling_df["__year"] = parsed_dates.dt.year

split_years = {
    "train": [2021, 2022, 2023, 2024],
    "validation": [2025],
    "test": [2026],
}

balanced_modeling_df = balance_binary_target_by_time(
    df=modeling_df,
    target_col=TARGET_COL,
    date_col=DATE_COL,
    split_years=split_years,
)

balanced_modeling_df.to_csv(cfg.MODELING_DATASET, index=False)

Unusefull_Variables = ["WeekEndingFriday",
                       "Ticker",
                       "AdjClosePrice_t+1"]
Unusefull_Variables.extend(
    col
    for col in modeling_df.columns
    if any(tag in str(col) for tag in ("EMO", "TEXTBLOB"))
)
X_train, y_train, X_validation, y_validation, X_test, y_test = split_data_by_date(
    cfg.MODELING_DATASET,
    TARGET_COL,
    Unusefull_Variables,
    DATE_COL,
)


# unione training_set e validation_set
X_train_full = pd.concat([X_train, X_validation], axis=0).reset_index(drop=True)
y_train_full = pd.concat([y_train, y_validation], axis=0).reset_index(drop=True)


"""
#MODIFICHE MOMENTANEE
include_vars = ["AdjClosePrice","AdjClosePrice_t-1","AdjClosePrice_t-2"]

X_train = X_train[include_vars]
X_validation = X_validation[include_vars]
X_train_full = X_train_full[include_vars]
X_test = X_test[include_vars]

print(X_test.head(5))

"""
