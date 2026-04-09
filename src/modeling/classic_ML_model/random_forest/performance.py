from sklearn.metrics import accuracy_score

from training_model import random_forest_model

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test

from validation import selected_variables


#adattamento covariate set alle variabili scelte
X_test_selected = X_test[selected_variables]


# prediction su test set
y_pred_test = random_forest_model.predict(X_test_selected)


# performance
test_accuracy = accuracy_score(y_test, y_pred_test)

print("Random Forest test accuracy:", test_accuracy)

#pareggiato/leggermente superiore al null model
