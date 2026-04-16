from pathlib import Path
import sys

import joblib
import pandas as pd

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full, y_train_full


current_dir = Path(__file__).resolve().parent
selected_variables_path = current_dir.parent / "lasso_model" / "selected_variables.csv"
model_path = current_dir / "logistic_model.joblib"


def load_selected_variables() -> list[str]:
    return pd.read_csv(selected_variables_path).iloc[:, 0].tolist()


def build_logistic_model():
    return Pipeline([
        ("scaler", StandardScaler()),
        ("model", LogisticRegression(
            max_iter=5000,
            random_state=42
        ))
    ])


def train_and_save_model():
    selected_variables = load_selected_variables()
    X_train_full_selected = X_train_full[selected_variables]

    logistic_model = build_logistic_model()
    logistic_model.fit(X_train_full_selected, y_train_full)
    joblib.dump(logistic_model, model_path)
    return logistic_model


if __name__ == "__main__":
    train_and_save_model()
    print("Model saved successfully.")
