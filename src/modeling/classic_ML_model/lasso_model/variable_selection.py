import warnings
from pathlib import Path
import sys

import joblib
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full
from training_model import train_and_save_model


current_dir = Path(__file__).resolve().parent
model_path = current_dir / "lasso_logistic_model.pkl"


def load_compatible_model():
    should_retrain = not model_path.exists()

    if not should_retrain:
        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter("always")
            lasso_logistic_model = joblib.load(model_path)

        has_version_warning = any(
            warning.category.__name__ == "InconsistentVersionWarning"
            for warning in caught_warnings
        )
        n_features = getattr(lasso_logistic_model, "n_features_in_", None)
        has_feature_mismatch = n_features != X_train_full.shape[1]

        if not has_version_warning and not has_feature_mismatch:
            return lasso_logistic_model

    return train_and_save_model()


lasso_logistic_model = load_compatible_model()


model = lasso_logistic_model.named_steps["model"]
coefs = model.coef_.flatten()
feature_names = getattr(lasso_logistic_model, "feature_names_in_", X_train_full.columns)


results = pd.DataFrame({
    "variable": feature_names,
    "coefficient": coefs
})


results["coefficient"] = results["coefficient"].where(
    results["coefficient"].abs() >= 0.2,
    0
)


selected_variables = results[results["coefficient"] != 0].copy()


print(selected_variables)
print("Numero variabili selezionate:", len(selected_variables))

selected_variables[["variable"]].to_csv(
    current_dir / "selected_variables.csv",
    index=False
)
