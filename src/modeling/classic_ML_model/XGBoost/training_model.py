from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full, y_train_full

from validation import best_params


#inizializzazione modello con i migliori iperparametri trovati
xgboost_model = XGBClassifier(
    **best_params,
    objective="binary:logistic",
    eval_metric="logloss",
    random_state=42,
    n_jobs=-1,
)

#training
xgboost_model.fit(X_train_full, y_train_full)