from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import config as cfg
from splitters import split_data_by_date

import pandas as pd

modeling_df = pd.read_csv(cfg.MODELING_DATASET)

Unusefull_Variables = ["WeekEndingFriday",
                       "Ticker",
                       "AdjClosePrice_t+1"]

# appendo a Unusefull_Variables tutte le colonne che contengono
# "EMO" oppure "TEXTBLOB" nel nome
Unusefull_Variables.extend(
    col
    for col in modeling_df.columns
    if any(tag in str(col) for tag in ("EMO", "TEXTBLOB"))
)
X_train, y_train, X_validation, y_validation, X_test, y_test = split_data_by_date(cfg.MODELING_DATASET , "AdjClosePrice_t+1_Up", Unusefull_Variables, "WeekEndingFriday")


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
