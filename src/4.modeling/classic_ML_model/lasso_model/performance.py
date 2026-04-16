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
    lasso_logistic_model = joblib.load(current_dir / "lasso_logistic_model.pkl")
    y_pred_test = lasso_logistic_model.predict(X_test)
    test_accuracy = float(accuracy_score(y_test, y_pred_test))

    with open(current_dir / "best_C.json", "r", encoding="utf-8") as f:
        best_c_results = json.load(f)

    selected_variables_path = current_dir / "selected_variables.csv"
    selected_variables_count = 0
    if selected_variables_path.exists():
        selected_variables_count = int(pd.read_csv(selected_variables_path).shape[0])

    performance = {
        "model": "lasso_logistic",
        "metric": "accuracy",
        "validation_accuracy": float(best_c_results["best_score"]),
        "best_C": best_c_results["best_C"],
        "selected_variables": selected_variables_count,
        "test_accuracy": test_accuracy,
    }

    with open(performance_path, "w", encoding="utf-8") as f:
        json.dump(performance, f, indent=4)

    print("Test accuracy:", test_accuracy)
    return performance


if __name__ == "__main__":
    evaluate_and_save_performance()
