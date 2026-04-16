from pathlib import Path
import json
import sys

import joblib
import pandas as pd
from sklearn.metrics import accuracy_score

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test


current_dir = Path(__file__).resolve().parent
performance_path = current_dir / "performance.json"


def evaluate_and_save_performance():
    selected_variables = pd.read_csv(
        current_dir.parent / "lasso_model" / "selected_variables.csv"
    ).iloc[:, 0].tolist()
    logistic_model = joblib.load(current_dir / "logistic_model.joblib")

    X_test_selected = X_test[selected_variables]
    y_pred_test = logistic_model.predict(X_test_selected)
    test_accuracy = float(accuracy_score(y_test, y_pred_test))

    performance = {
        "model": "logistic_regression",
        "metric": "accuracy",
        "selected_variables": len(selected_variables),
        "test_accuracy": test_accuracy,
    }

    with open(performance_path, "w", encoding="utf-8") as f:
        json.dump(performance, f, indent=4)

    print("Test accuracy:", test_accuracy)
    print("Numero variabili utilizzate:", len(selected_variables))
    return performance


if __name__ == "__main__":
    evaluate_and_save_performance()
