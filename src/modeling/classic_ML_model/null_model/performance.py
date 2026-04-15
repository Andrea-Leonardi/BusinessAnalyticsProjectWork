from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test

from sklearn.metrics import accuracy_score, balanced_accuracy_score
import joblib

current_dir = Path(__file__).resolve().parent
null_model = joblib.load(current_dir / "null_model.joblib")

# predizioni sul test set
y_pred_test = null_model.predict(X_test)
predicted_class = y_pred_test[0] if len(y_pred_test) > 0 else None

# calcolo accuracy
accuracy = accuracy_score(y_test, y_pred_test)
balanced_accuracy = balanced_accuracy_score(y_test, y_pred_test)

print("Class predicted by the null model:", predicted_class)
print("Test accuracy:", accuracy)
print("Test balanced accuracy:", balanced_accuracy)

import numpy as np
print(np.mean(y_pred_test))
