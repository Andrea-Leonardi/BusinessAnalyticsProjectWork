from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test

from training_model import logistic_model, selected_variables

from sklearn.metrics import accuracy_score


X_test_selected = X_test[selected_variables]

y_pred_test = logistic_model.predict(X_test_selected)

test_accuracy = accuracy_score(y_test, y_pred_test)

print("Test accuracy:", test_accuracy) #inferiore a null model ma superiore a lasso model
print("Numero variabili utilizzate:", len(selected_variables))

