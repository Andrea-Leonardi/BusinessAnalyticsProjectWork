from sklearn.metrics import accuracy_score


from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test

import json
import joblib


input_dir = Path(__file__).resolve().parent

xgboost_model = joblib.load(
    input_dir / "xgboost_model.joblib"
)

#adattamento covariate set alle variabili scelte

with open(input_dir / "best_params.json", "r") as f:
    results = json.load(f)

selected_variables = results.get("selected_variables")

if selected_variables is None:
    selected_variables = pd.read_csv(
        input_dir.parent / "lasso_model" / "selected_variables.csv"
    ).iloc[:, 0].tolist()

X_test_selected = X_test[selected_variables]


# prediction su test set
y_pred_test = xgboost_model.predict(X_test_selected)

# performance
test_accuracy = accuracy_score(y_test, y_pred_test)

print("XGBoost test accuracy:", test_accuracy)
