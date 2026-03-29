import numpy as np
from sklearn.model_selection import GridSearchCV

from training_model import pipeline

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train, y_train, X_validation, y_validation
from sklearn.metrics import balanced_accuracy_score





from sklearn.model_selection import GridSearchCV, PredefinedSplit
import numpy as np

X = np.vstack([X_train, X_validation])
y = np.concatenate([y_train, y_validation])

split_index = [-1]*len(X_train) + [0]*len(X_validation)


ps = PredefinedSplit(test_fold=split_index)


grid = GridSearchCV(
    pipeline,
    param_grid={"model__C": np.arange(0.1,0.3,0.1)},
    scoring="balanced_accuracy",
    cv=ps
)


grid.fit(X, y)

best_C = grid.best_params_["model__C"]
