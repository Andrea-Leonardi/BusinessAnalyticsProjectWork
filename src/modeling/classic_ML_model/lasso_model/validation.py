from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression

from pathlib import Path
import sys
import json

from sklearn.metrics import accuracy_score

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train, y_train, X_validation, y_validation


current_dir = Path(__file__).resolve().parent

pipeline = Pipeline([
    ("scaler", StandardScaler()),
    ("model", LogisticRegression(
        l1_ratio=1.0,
        solver="saga",
        max_iter=7000,
        random_state=42
    ))
])


"""
param_grid = {
    "model__C": [
        1e5, 3e4,
        1e4, 3e3,
        1e3, 3e2,
        1e2, 3e1,
        1e1, 3,
        1, 0.5, 0.45, 0.4, 0.37, 0.35, 0.34, 0.33, 0.32, 0.3,
        0.1, 0.01
    ]
}
"""

param_grid = {
    "model__C": [
        1,
        0.3, 0.1, 0.05, 0.03, 0.02, 0.015, 0.01,
        0.007, 0.005, 0.003, 0.002, 0.001,
        0.0007, 0.0005, 0.0003, 0.0001
    ]
}


best_score = -1
best_C = None
best_nonzero_count = None


scores = {}

for C in param_grid["model__C"]:

    pipeline.set_params(model__C = C)

    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_validation)

    score = accuracy_score(y_validation, y_pred)
    nonzero_count = int((pipeline.named_steps["model"].coef_.ravel() != 0).sum())
    scores[C] = score

    is_better_model = False
    if best_nonzero_count is None:
        is_better_model = True
    elif nonzero_count > 0 and best_nonzero_count == 0:
        is_better_model = True
    elif nonzero_count == 0 and best_nonzero_count > 0:
        is_better_model = False
    elif score > best_score:
        is_better_model = True
    elif score == best_score and best_C is not None and C < best_C:
        is_better_model = True

    if is_better_model:
        best_score = score
        best_C = C
        best_nonzero_count = nonzero_count



print(scores)
print(best_C, best_score, best_nonzero_count)




# salvataggio risultati
with open(current_dir / "best_C.json", "w") as f:
    json.dump(
        {
            "best_C": best_C,
            "best_score": best_score,
            "best_nonzero_count": best_nonzero_count,
            "metric": "accuracy",
            "scores": scores
        },
        f,
        indent=4
    )






