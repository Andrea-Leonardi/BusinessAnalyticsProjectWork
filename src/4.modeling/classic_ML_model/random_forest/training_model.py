from sklearn.ensemble import RandomForestClassifier

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full, y_train_full


import json
import joblib
import pandas as pd


output_dir = Path(__file__).resolve().parent
selected_variables_path = output_dir.parent / "lasso_model" / "selected_variables.csv"

# carico best_params salvato da validation.py 
with open(output_dir / "best_params.json", "r") as f: results = json.load(f)


best_params = results["best_params"]
selected_variables = pd.read_csv(selected_variables_path).iloc[:, 0].tolist()


#inizializzazione modello con i migliori iperparametri trovati

random_forest_model = RandomForestClassifier(
    **best_params,
    random_state=42,
    n_jobs=1,
)

#adattamento covariate set alle variabili scelte
X_train_selected_full = X_train_full[selected_variables]

#training
random_forest_model.fit(X_train_selected_full, y_train_full)


joblib.dump(random_forest_model, output_dir / "random_forest_model.joblib")

print("Modello addestrato e salvato correttamente.")

"""
"""
