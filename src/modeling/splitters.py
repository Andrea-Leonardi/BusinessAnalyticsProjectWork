import pandas as pd
from typing import List, Optional
from sklearn.model_selection import train_test_split


def split_data_by_date(
    csv_path: str,
    response_var: str,
    exclude_vars: Optional[List[str]] = None,
    date_col: str = "WeekEndingFriday"
):

    if exclude_vars is None:
        exclude_vars = []

    df = pd.read_csv(csv_path)
    df[date_col] = pd.to_datetime(df[date_col])

    df = df.dropna()


    #training
    df_train = df[
        (df[date_col].dt.year >= 2021) &
        (df[date_col].dt.year <= 2024)
    ]

    X_train = df_train.drop(columns=[response_var] + exclude_vars, errors="ignore")
    y_train = df_train[response_var]


    #validation
    df_validation = df[
        (df[date_col].dt.year == 2025)
    ]

    X_validation = df_validation.drop(columns=[response_var] + exclude_vars, errors="ignore")
    y_validation = df_validation[response_var]


    #test
    df_test = df[
        (df[date_col].dt.year == 2026)
    ]

    X_test = df_test.drop(columns=[response_var] + exclude_vars, errors="ignore")
    y_test = df_test[response_var]


    return X_train, y_train, X_validation, y_validation, X_test, y_test


#togliere ticker e date (ANCORA NON FATTO !!!!!!!!!!)
#flessibilizzare le date?
