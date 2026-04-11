from sklearn.ensemble import RandomForestClassifier

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full, y_train_full


import json
import joblib


output_dir = Path(__file__).resolve().parent

# carico best_params salvato da validation.py 
with open(output_dir / "best_params.json", "r") as f: results = json.load(f)


best_params = results["best_params"]
selected_variables = results["selected_variables"]


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


joblib.dump(random_forest_model, output_dir / "random_forest_model.joblib")

print("Modello addestrato e salvato correttamente.")

"""
"""