from pathlib import Path
import sys

from pandas import read_csv


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import config as cfg
from splitters import split_data_by_date

import pandas as pd

Unusefull_Variables = ["WeekEndingFriday",
                       "Ticker",
                       "AdjClosePrice_t+1"]

#appendo a unusefull_variables tutte le variabili di modeling che hanno EMO nel nome
for col in cfg.MODELING_DATASET.columns:
    if "EMO" in col:
        Unusefull_Variables.append(col) 

for col in cfg.MODELING_DATASET.columns:
    if "TEXTBLOB" in col:
        Unusefull_Variables.append(col) 


X_train, y_train, X_validation, y_validation, X_test, y_test = split_data_by_date(cfg.MODELING_DATASET , "AdjClosePrice_t+1_Up", ["WeekEndingFriday","Ticker","AdjClosePrice_t+1"], "WeekEndingFriday")


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
