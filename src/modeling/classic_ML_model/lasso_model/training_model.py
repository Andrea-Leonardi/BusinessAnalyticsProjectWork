from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full, y_train_full

import json
import joblib

import numpy as np
import pandas as pd


from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression




current_dir = Path(__file__).resolve().parent

# carico best_alpha salvato da validation.py

with open(current_dir / "best_alpha.json", "r") as f:
    validation_results = json.load(f)

best_alpha = validation_results["best_alpha"]


lasso_logistic_model = Pipeline([
    ("scaler", StandardScaler()),
    ("model", LogisticRegression(
        penalty="l1",
        C=1/best_alpha,  
        solver="saga",
        max_iter=7000,            # tra 5000 e 10000
        #tol=1e-3,
        random_state=42
    ))
])

#addestramento
lasso_logistic_model.fit(X_train_full, y_train_full)


# salvataggio modello
joblib.dump(lasso_logistic_model, current_dir / "lasso_logistic_model.pkl")

print("Model saved successfully.")
