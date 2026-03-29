from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression


import numpy as np
from sklearn.model_selection import GridSearchCV



param_grid = {
    "C": np.arange(0.1, 1.1, 0.1)   #lambda non usabile;   C = 1/lambda
}
    

models = {}

for C in param_grid["C"]:
    model = Pipeline([
        ("scaler", StandardScaler()),
        ("model", LogisticRegression(
            penalty="l1",
            solver="saga",
            C=C,
            max_iter=5000,
            random_state=42
        ))
    ])

    models[C] = model



print(models)


# lasso_modello = lasso_logit.fit(X_train, y_train)  #non accetta valori nulli !!!