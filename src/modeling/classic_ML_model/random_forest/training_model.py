from sklearn.ensemble import RandomForestClassifier

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full, y_train_full

from validation import best_params, selected_variables, X_train_selected, X_validation_selected 


#inizializzazione modello con i migliori iperparametri trovati

random_forest_model = RandomForestClassifier(
    **best_params,
    random_state=42,
    n_jobs=-1,
)

#adattamento covariate set alle variabili scelte
X_train_selected_full = X_train_full[selected_variables]

#training
random_forest_model.fit(X_train_selected_full, y_train_full)