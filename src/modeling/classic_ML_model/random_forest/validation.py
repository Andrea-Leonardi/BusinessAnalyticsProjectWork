from pathlib import Path
import sys
import itertools
import copy

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train, y_train, X_validation, y_validation

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score




# griglia iperparametri

param_grid = { 

    "n_estimators": [500],
    "max_depth":  [3], #, 5, 8], 
    "min_samples_leaf" : [20], #[5], , None
    "max_features":["sqrt", 0.3, 0.5] #["sqrt"] 

}   



# inizializzazione

best_score = -1
best_params = None
best_model = None
scores = {}


#definizione training set e validation set  <--- variabili scelte
selected_variables = pd.read_csv(
    "src/modeling/classic_ML_model/lasso_model/selected_variables.csv"
).iloc[:, 0].tolist()

X_train_selected = X_train[selected_variables]
X_validation_selected = X_validation[selected_variables]


# loop su tutte le combinazioni

for n_estimators, max_depth,min_samples_leaf, max_features in itertools.product(
    param_grid["n_estimators"],
    param_grid["max_depth"],
    param_grid["min_samples_leaf"],
    param_grid["max_features"],
):
    random_forest_model = RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        min_samples_leaf=min_samples_leaf,
        max_features=max_features,
        random_state=42,
        n_jobs=-1,
    )

    random_forest_model.fit(X_train_selected, y_train)

    y_pred_validation = random_forest_model.predict(X_validation_selected)

    score = accuracy_score(y_validation, y_pred_validation)

    params = {
        "n_estimators": n_estimators,
        "max_depth": max_depth,
        "min_samples_leaf": min_samples_leaf,
        "max_features": max_features,
    }

    scores[str(params)] = score

    if score > best_score:
        best_score = score
        best_params = params
        best_model = copy.deepcopy(random_forest_model)





# risultati

scores_df = pd.DataFrame(
    [
        {"params": k, "balanced_accuracy": v}
        for k, v in scores.items()
    ]
).sort_values("balanced_accuracy", ascending=False)

print(scores_df)
print("\nBest params:", best_params)
print("Best validation accuracy:", best_score)
"""
"""