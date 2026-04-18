from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full, y_train_full, get_model_output_dir

import json
import joblib

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression


output_dir = get_model_output_dir(Path(__file__).resolve().parent.name)


def load_best_c():
    with open(output_dir / "best_C.json", "r") as f:
        validation_results = json.load(f)
    return validation_results["best_C"]


def build_lasso_logistic_model(best_c):
    return Pipeline([
        ("scaler", StandardScaler()),
        ("model", LogisticRegression(
            penalty="l1",
            C=best_c,
            solver="saga",
            max_iter=7000,
            random_state=42
        ))
    ])


def train_and_save_model():
    best_c = load_best_c()
    lasso_logistic_model = build_lasso_logistic_model(best_c)
    lasso_logistic_model.fit(X_train_full, y_train_full)
    joblib.dump(lasso_logistic_model, output_dir / "lasso_logistic_model.pkl")
    return lasso_logistic_model


if __name__ == "__main__":
    train_and_save_model()
    print("Model saved successfully.")
