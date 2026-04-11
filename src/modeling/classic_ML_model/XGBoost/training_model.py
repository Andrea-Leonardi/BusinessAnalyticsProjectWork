from xgboost import XGBClassifier


from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full, y_train_full

#from validation import best_params

import json
import joblib


output_dir = Path(__file__).resolve().parent

# carico best_params salvato da validation.py 
with open(output_dir / "best_params.json", "r") as f: results = json.load(f)

best_params = results["best_params"]
selected_variables = results.get("selected_variables")

if selected_variables is None:
    selected_variables = pd.read_csv(
        output_dir.parent / "lasso_model" / "selected_variables.csv"
    ).iloc[:, 0].tolist()



#inizializzazione modello con i migliori iperparametri trovati
xgboost_model = XGBClassifier(
    **best_params,
    objective="binary:logistic",
    eval_metric="logloss",
    random_state=42,
    n_jobs=-1,
)

#adattamento covariate set alle variabili scelte
X_train_selected_full = X_train_full[selected_variables]

#training
xgboost_model.fit(X_train_selected_full, y_train_full)


joblib.dump(xgboost_model, output_dir / "xgboost_model.joblib")

print("Modello addestrato e salvato correttamente.")
