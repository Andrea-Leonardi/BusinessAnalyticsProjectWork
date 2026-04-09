from pathlib import Path
import sys

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full, y_train_full

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score


selected_variables = pd.read_csv(
    "src/modeling/classic_ML_model/lasso_model/selected_variables.csv"
).iloc[:, 0].tolist()



X_train_full_selected = X_train_full[selected_variables]



logistic_model = Pipeline([
    ("scaler", StandardScaler()),
    ("model", LogisticRegression(
        penalty=None,
        max_iter=5000,
        random_state=42
    ))
])

logistic_model.fit(X_train_full_selected, y_train_full)
