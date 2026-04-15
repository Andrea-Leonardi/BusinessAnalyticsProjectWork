from sklearn.metrics import accuracy_score, balanced_accuracy_score


from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test

import json
import joblib


input_dir = Path(__file__).resolve().parent
selected_variables_path = input_dir.parent / "lasso_model" / "selected_variables.csv"

xgboost_model = joblib.load(
    input_dir / "xgboost_model.joblib"
)

#adattamento covariate set alle variabili scelte

selected_variables = pd.read_csv(selected_variables_path).iloc[:, 0].tolist()

X_test_selected = X_test[selected_variables]


# prediction su test set
y_pred_test = xgboost_model.predict(X_test_selected)

# performance
test_accuracy = accuracy_score(y_test, y_pred_test)
test_balanced_accuracy = balanced_accuracy_score(y_test, y_pred_test)

print("XGBoost test accuracy:", test_accuracy)
print("XGBoost test balanced accuracy:", test_balanced_accuracy)
