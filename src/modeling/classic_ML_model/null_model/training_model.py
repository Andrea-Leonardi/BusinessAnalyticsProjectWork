from pathlib import Path
import sys

import joblib
from sklearn.dummy import DummyClassifier

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full, y_train_full


current_dir = Path(__file__).resolve().parent
model_path = current_dir / "null_model.joblib"


def build_null_model():
    return DummyClassifier(strategy="most_frequent")


def train_and_save_model():
    null_model = build_null_model()
    null_model.fit(X_train_full, y_train_full)
    joblib.dump(null_model, model_path)
    return null_model


if __name__ == "__main__":
    train_and_save_model()
    print("Model saved successfully.")
