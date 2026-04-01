import pandas as pd
import numpy as np

from training_model import X_train_full, lasso_logistic_model


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


# filtro variabili sopravvissute alla LASSO e ordino per imposrtanza assoluta
selected_variables = results[results["coefficient"] != 0]
selected_variables = selected_variables.reindex(
    selected_variables["coefficient"].abs().sort_values(ascending=False).index
)

# filtro variabili escluse dalla LASSO
excluded_variables = results[results["coefficient"] == 0]



"""
print("selected_variables)
print(excluded_variables)
"""