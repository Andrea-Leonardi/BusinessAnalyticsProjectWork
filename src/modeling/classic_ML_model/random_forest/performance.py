from sklearn.metrics import accuracy_score

from training_model import random_forest_model

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test


import json
import joblib


input_dir = Path(__file__).resolve().parent

random_forest_model = joblib.load(
    input_dir / "random_forest_model.joblib"
)

#adattamento covariate set alle variabili scelte

with open(input_dir / "best_params.json", "r") as f:
    results = json.load(f)

selected_variables = results["selected_variables"]

X_test_selected = X_test[selected_variables]


# prediction su test set
y_pred_test = random_forest_model.predict(X_test_selected)


# performance
test_accuracy = accuracy_score(y_test, y_pred_test)

print("Random Forest test accuracy:", test_accuracy)

#pareggiato al logistic regression
