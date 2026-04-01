from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test

from sklearn.metrics import accuracy_score

from training_model import lasso_logistic_model 

# predizioni sul test set
y_pred_test = lasso_logistic_model.predict(X_test)

# accuracy
test_accuracy = accuracy_score(y_test, y_pred_test)

print("Test accuracy:", test_accuracy)