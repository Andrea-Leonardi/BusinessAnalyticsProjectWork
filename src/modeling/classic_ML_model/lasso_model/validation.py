import numpy as np
from sklearn.model_selection import GridSearchCV

from training_model import pipeline

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train, y_train, X_validation, y_validation
from sklearn.metrics import balanced_accuracy_score




param_grid = {
    "model__C": np.arange(0.1, 0.4, 0.1)
}

best_score = -1
best_C = None
best_model = None


for C in param_grid["model__C"]:

    pipeline.set_params(model__C=C)

    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_validation)

    score = balanced_accuracy_score(y_validation, y_pred)

    if score > best_score:
        best_score = score
        best_C = C
        best_model = pipeline



print(best_C, best_score)
