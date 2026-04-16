from pathlib import Path
import json
import sys

import joblib
import pandas as pd
from sklearn.metrics import accuracy_score

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test


input_dir = Path(__file__).resolve().parent
selected_variables_path = input_dir.parent / "lasso_model" / "selected_variables.csv"
performance_path = input_dir / "performance.json"


def evaluate_and_save_performance():
    xgboost_model = joblib.load(input_dir / "xgboost_model.joblib")
    selected_variables = pd.read_csv(selected_variables_path).iloc[:, 0].tolist()

    with open(input_dir / "best_params.json", "r", encoding="utf-8") as f:
        validation_results = json.load(f)

    X_test_selected = X_test[selected_variables]
    y_pred_test = xgboost_model.predict(X_test_selected)
    test_accuracy = float(accuracy_score(y_test, y_pred_test))

    performance = {
        "model": "xgboost",
        "metric": "accuracy",
        "validation_accuracy": float(validation_results["best_score"]),
        "best_params": validation_results["best_params"],
        "selected_variables": len(selected_variables),
        "test_accuracy": test_accuracy,
    }

    with open(performance_path, "w", encoding="utf-8") as f:
        json.dump(performance, f, indent=4)

    print("XGBoost test accuracy:", test_accuracy)
    return performance


if __name__ == "__main__":
    evaluate_and_save_performance()
