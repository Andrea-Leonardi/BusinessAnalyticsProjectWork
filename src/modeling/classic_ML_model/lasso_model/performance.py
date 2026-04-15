from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test

from sklearn.metrics import accuracy_score, balanced_accuracy_score

import joblib

current_dir = Path(__file__).resolve().parent

# carico il modello già addestrato
lasso_logistic_model = joblib.load(current_dir / "lasso_logistic_model.pkl")

# predizioni sul test set
y_pred_test = lasso_logistic_model.predict(X_test)

# performance
test_accuracy = accuracy_score(y_test, y_pred_test)
test_balanced_accuracy = balanced_accuracy_score(y_test, y_pred_test)

print("Test accuracy:", test_accuracy)
print("Test balanced accuracy:", test_balanced_accuracy)



print(lasso_logistic_model.named_steps["model"].intercept_)

import numpy as np
print(np.mean(y_pred_test))
