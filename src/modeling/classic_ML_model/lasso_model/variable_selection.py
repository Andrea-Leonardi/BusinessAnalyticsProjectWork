import pandas as pd
import numpy as np

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import joblib

from split_data import X_train_full




current_dir = Path(__file__).resolve().parent

# carico il modello già addestrato
lasso_logistic_model = joblib.load(current_dir / "lasso_logistic_model.pkl")


# estrazione  modello logistico dalla pipeline
model = lasso_logistic_model.named_steps["model"]


# vettore coefficienti stimati
coefs = model.coef_.flatten()

# nomi delle variabili (assumendo X_train_full sia DataFrame)
feature_names = X_train_full.columns


# variabile e coefficiente stimato
results = pd.DataFrame({
    "variable": feature_names,
    "coefficient": coefs
})


# porto a zero i coefficienti con valore assoluto minore di 0.05
results["coefficient"] = results["coefficient"].where(
    results["coefficient"].abs() >= 0.2, #0.05 
    0
)



# variabili sopravvissute
selected_variables = results[results["coefficient"] != 0].copy()


print(selected_variables)
print("Numero variabili selezionate:", len(selected_variables))

#salvo su csv
selected_variables[["variable"]].to_csv(
    "src/modeling/classic_ML_model/lasso_model/selected_variables.csv",
    index=False
)

"""
"""