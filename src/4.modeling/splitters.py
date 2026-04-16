import pandas as pd
from typing import List, Optional


def split_temporal_dataframes(
    df: pd.DataFrame,
    date_col: str = "WeekEndingFriday",
):
    working_df = df.copy()
    working_df[date_col] = pd.to_datetime(working_df[date_col])
    working_df = working_df.dropna().reset_index(drop=True)

    df_train = working_df[
        (working_df[date_col].dt.year >= 2021) &
        (working_df[date_col].dt.year <= 2024)
    ].copy()

    df_validation = working_df[
        (working_df[date_col].dt.year == 2025)
    ].copy()

    df_test = working_df[
        (working_df[date_col].dt.year == 2026)
    ].copy()

    return df_train, df_validation, df_test


def split_dataframe_by_date(
    df: pd.DataFrame,
    response_var: str,
    exclude_vars: Optional[List[str]] = None,
    date_col: str = "WeekEndingFriday"
):

    if exclude_vars is None:
        exclude_vars = []

    df_train, df_validation, df_test = split_temporal_dataframes(df, date_col=date_col)

    X_train = df_train.drop(columns=[response_var] + exclude_vars, errors="ignore")
    y_train = df_train[response_var]

    X_validation = df_validation.drop(columns=[response_var] + exclude_vars, errors="ignore")
    y_validation = df_validation[response_var]

    X_test = df_test.drop(columns=[response_var] + exclude_vars, errors="ignore")
    y_test = df_test[response_var]

    return X_train, y_train, X_validation, y_validation, X_test, y_test


def split_data_by_date(
    csv_path: str,
    response_var: str,
    exclude_vars: Optional[List[str]] = None,
    date_col: str = "WeekEndingFriday"
):

    df = pd.read_csv(csv_path)
    return split_dataframe_by_date(
        df=df,
        response_var=response_var,
        exclude_vars=exclude_vars,
        date_col=date_col,
    )

