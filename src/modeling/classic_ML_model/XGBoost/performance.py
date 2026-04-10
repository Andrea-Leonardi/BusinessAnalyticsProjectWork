from sklearn.metrics import accuracy_score

from training_model import xgboost_model

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test

# prediction su test set
y_pred_test = xgboost_model.predict(X_test)

# performance
test_accuracy = accuracy_score(y_test, y_pred_test)

print("XGBoost test accuracy:", test_accuracy)

