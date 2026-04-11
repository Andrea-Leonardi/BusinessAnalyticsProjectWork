from pathlib import Path
import sys
import itertools
import copy

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train, y_train, X_validation, y_validation

from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score

import json
import copy


"""
"""


# griglia iperparametri

param_grid = {

    "n_estimators": [50, 75, 100],

    "learning_rate": [0.004, 0.005, 0.006, 0.007],

    "max_depth": [5, 6, 7, 8, 9],

    "min_child_weight": [4, 5, 6, 7],

    "subsample": [0.1, 0.2, 0.3, 0.4, 0.5], 

    "colsample_bytree": [0.7, 0.8, 0.9, 1],

}



# inizializzazione

best_score = -1
best_params = None
best_model = None
scores = {}



#definizione training set e validation set  <--- variabili scelte
selected_variables = pd.read_csv(
    "src/modeling/classic_ML_model/lasso_model/selected_variables.csv"
).iloc[:, 0].tolist()

X_train_selected = X_train[selected_variables]
X_validation_selected = X_validation[selected_variables]


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

    xgboost_model.fit(X_train_selected, y_train)

    y_pred_validation = xgboost_model.predict(X_validation_selected)

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




# salvataggio risultati

output_dir = Path(__file__).resolve().parent

with open(output_dir / "best_params.json", "w") as f:
    json.dump(
        {
            "best_params": best_params,
            "best_score": best_score,
            "scores": scores,
            "selected_variables": selected_variables
        },
        f,
        indent=4
    )

print(scores_df)
print("\nBest params:", best_params)
print("Best validation accuracy:", best_score)
