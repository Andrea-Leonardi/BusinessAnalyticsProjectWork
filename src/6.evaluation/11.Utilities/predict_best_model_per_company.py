from pathlib import Path
import sys

import numpy as np
import pandas as pd
import torch
from sklearn.preprocessing import StandardScaler


CURRENT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(CURRENT_DIR))
sys.path.insert(0, str(CURRENT_DIR.parents[1]))
sys.path.insert(0, str(CURRENT_DIR.parents[0]))

from best_model import MODELING_DIR, get_best_model_bundle
from evaluation_data_prep import build_sector_test_data


UTILITIES_SECTOR_CODE = 11
TARGET_COL = "AdjClosePrice_t+1_Up"
DATE_COL = "WeekEndingFriday"
OUTPUT_CSV = CURRENT_DIR / "best_model_predictions_per_company.csv"


def load_selected_variables() -> list[str] | None:
    selected_variables_path = MODELING_DIR / "lasso_model" / "selected_variables.csv"
    if not selected_variables_path.exists():
        return None
    return pd.read_csv(selected_variables_path).iloc[:, 0].tolist()


def predict_with_model(model_bundle: dict, X_test: pd.DataFrame) -> np.ndarray:
    model_name = model_bundle["best_model_entry"]["model"]
    best_model = model_bundle["best_model"]

    if model_name == "null_model":
        return best_model.predict(X_test)

    model_feature_names = getattr(best_model, "feature_names_in_", None)
    if model_feature_names is not None:
        return best_model.predict(X_test[list(model_feature_names)])

    selected_variables = load_selected_variables()
    if not selected_variables:
        raise ValueError("Selected variables file not found or empty for Utilities.")

    X_test_selected = X_test[selected_variables]

    if model_name == "neural_network":
        checkpoint = torch.load(
            MODELING_DIR / "neural network" / "neural_network_model.pt",
            map_location="cpu",
            weights_only=True,
        )
        scaler = StandardScaler()
        scaler.mean_ = np.asarray(checkpoint["scaler_mean"], dtype=np.float64)
        scaler.scale_ = np.asarray(checkpoint["scaler_scale"], dtype=np.float64)
        scaler.var_ = scaler.scale_ ** 2
        scaler.n_features_in_ = len(selected_variables)
        scaler.feature_names_in_ = np.asarray(selected_variables, dtype=object)

        X_test_scaled = scaler.transform(X_test_selected)
        X_test_tensor = torch.tensor(X_test_scaled, dtype=torch.float32)

        with torch.no_grad():
            logits = best_model(X_test_tensor)
            return torch.argmax(logits, dim=1).cpu().numpy()

    return best_model.predict(X_test_selected)


def build_prediction_comparison_dataframe() -> pd.DataFrame:
    model_bundle = get_best_model_bundle()
    utilities_test_df, X_test_utilities = build_sector_test_data(UTILITIES_SECTOR_CODE)
    y_pred_best_model = predict_with_model(model_bundle, X_test_utilities)

    comparison_df = utilities_test_df[["Ticker", DATE_COL, TARGET_COL]].copy()
    comparison_df["predicted_" + TARGET_COL] = y_pred_best_model
    return comparison_df


def main():
    comparison_df = build_prediction_comparison_dataframe()
    comparison_df.to_csv(OUTPUT_CSV, index=False)
    print(
        comparison_df[
            ["Ticker", DATE_COL, TARGET_COL, "predicted_" + TARGET_COL]
        ].to_string(index=False)
    )
    print(f"CSV salvato in: {OUTPUT_CSV}")
    print(f"Numero di ticker distinti: {comparison_df['Ticker'].nunique()}")


if __name__ == "__main__":
    main()
