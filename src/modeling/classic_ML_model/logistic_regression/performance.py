from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test

from sklearn.metrics import accuracy_score, balanced_accuracy_score
import joblib
import pandas as pd


current_dir = Path(__file__).resolve().parent
selected_variables = pd.read_csv(
    current_dir.parent / "lasso_model" / "selected_variables.csv"
).iloc[:, 0].tolist()
logistic_model = joblib.load(current_dir / "logistic_model.joblib")


X_test_selected = X_test[selected_variables]

y_pred_test = logistic_model.predict(X_test_selected)

test_accuracy = accuracy_score(y_test, y_pred_test)
test_balanced_accuracy = balanced_accuracy_score(y_test, y_pred_test)

print("Test accuracy:", test_accuracy) #inferiore a null model ma superiore a lasso model
print("Test balanced accuracy:", test_balanced_accuracy)
print("Numero variabili utilizzate:", len(selected_variables))

