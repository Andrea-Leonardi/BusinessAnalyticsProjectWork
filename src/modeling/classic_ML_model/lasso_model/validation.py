from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDClassifier


import numpy as np
from sklearn.model_selection import GridSearchCV


from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train, y_train, X_validation, y_validation
from sklearn.metrics import accuracy_score




pipeline = Pipeline([
    ("scaler", StandardScaler()),
    ("model", SGDClassifier(
        loss="log_loss",      
        penalty="l1",                  
        max_iter=10000,
        #tol=1e-3,
        random_state=42
    ))
])



param_grid = {
    "model__alpha": [
        1e-5, 3e-5,
        1e-4, 3e-4,
        1e-3, 3e-3,
        1e-2, 3e-2,
        1e-1, 3e-1,
        1, 2 ,2.1, 2.2, 2.5, 2.7, 2.8, 2.9, 3, 10, 100
    ]
}

best_score = -1
best_alpha = None


scores = {}

for alpha in param_grid["model__alpha"]:

    pipeline.set_params(model__alpha=alpha)

    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_validation)

    score = accuracy_score(y_validation, y_pred)
    scores[alpha] = score

    if score > best_score:
        best_score = score
        best_alpha = alpha
        




print(scores)
print(best_alpha, best_score)








