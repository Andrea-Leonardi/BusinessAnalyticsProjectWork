from pathlib import Path
import sys
import itertools
import copy

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train, y_train, X_validation, y_validation

from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score


"""
USATE TUTTE LE VARIABILI NON SOLO QUELLE SELEZIONATE

"""


# griglia iperparametri

param_grid = {
    "n_estimators": [100, 200],
    "learning_rate": [0.0001 ,0.0005],
    "max_depth": [8],
    "min_child_weight": [5],
    "subsample": [0.7, 1.0],
    "colsample_bytree": [0.3, 0.5],
}



# inizializzazione

best_score = -1
best_params = None
best_model = None
scores = {}


# loop su tutte le combinazioni

for (
    n_estimators,
    learning_rate,
    max_depth,
    min_child_weight,
    subsample,
    colsample_bytree,
) in itertools.product(
    param_grid["n_estimators"],
    param_grid["learning_rate"],
    param_grid["max_depth"],
    param_grid["min_child_weight"],
    param_grid["subsample"],
    param_grid["colsample_bytree"],
):
    xgboost_model = XGBClassifier(
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        max_depth=max_depth,
        min_child_weight=min_child_weight,
        subsample=subsample,
        colsample_bytree=colsample_bytree,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=42,
        n_jobs=-1,
    )

    xgboost_model.fit(X_train, y_train)

    y_pred_validation = xgboost_model.predict(X_validation)

    score = accuracy_score(y_validation, y_pred_validation)

    params = {
        "n_estimators": n_estimators,
        "learning_rate": learning_rate,
        "max_depth": max_depth,
        "min_child_weight": min_child_weight,
        "subsample": subsample,
        "colsample_bytree": colsample_bytree,
    }

    scores[str(params)] = score

    if score > best_score:
        best_score = score
        best_params = params
        best_model = copy.deepcopy(xgboost_model)




# risultati

scores_df = pd.DataFrame(
    [
        {"params": k, "accuracy": v}
        for k, v in scores.items()
    ]
).sort_values("accuracy", ascending=False)

print(scores_df)
print("\nBest params:", best_params)
print("Best validation accuracy:", best_score)