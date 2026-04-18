import json
from pathlib import Path

import joblib
import torch
import torch.nn as nn


CURRENT_DIR = Path(__file__).resolve().parent
MODELING_DIR = CURRENT_DIR.parents[1] / "4.modeling" / "classic_ML_model" / "10.Technology"
MODEL_COMPARISON_PATH = MODELING_DIR / "orchestrator_results" / "model_comparison.json"
EXCLUDED_MODELS = {"always_zero", "always_one"}


class NeuralNet(nn.Module):
    def __init__(self, input_dim, hidden_dim_1, hidden_dim_2, dropout):
        super().__init__()
        self.model = nn.Sequential(
            nn.Linear(input_dim, hidden_dim_1),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim_1, hidden_dim_2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim_2, 2),
        )

    def forward(self, x):
        return self.model(x)


def load_model_comparison() -> dict:
    with open(MODEL_COMPARISON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_best_entry(model_comparison: dict) -> dict:
    eligible_models = [
        item for item in model_comparison["ranking"] if item["model"] not in EXCLUDED_MODELS
    ]
    if not eligible_models:
        raise ValueError("No eligible models found for Technology.")
    return max(eligible_models, key=lambda item: item["test_accuracy"])


def get_null_model_entry(model_comparison: dict) -> dict:
    for item in model_comparison["ranking"]:
        if item["model"] == "null_model":
            return item
    raise ValueError("null_model not found in model comparison results.")


def load_trained_model(model_name: str):
    if model_name == "null_model":
        return joblib.load(MODELING_DIR / "null_model" / "null_model.joblib")
    if model_name == "lasso_logistic":
        return joblib.load(MODELING_DIR / "lasso_model" / "lasso_logistic_model.pkl")
    if model_name == "logistic_regression":
        return joblib.load(MODELING_DIR / "logistic_regression" / "logistic_model.joblib")
    if model_name == "random_forest":
        return joblib.load(MODELING_DIR / "random_forest" / "random_forest_model.joblib")
    if model_name == "xgboost":
        return joblib.load(MODELING_DIR / "XGBoost" / "xgboost_model.joblib")
    if model_name == "neural_network":
        checkpoint = torch.load(
            MODELING_DIR / "neural network" / "neural_network_model.pt",
            map_location="cpu",
            weights_only=True,
        )
        best_params = checkpoint["best_params"]
        model = NeuralNet(
            input_dim=checkpoint["input_dim"],
            hidden_dim_1=best_params["hidden_dim_1"],
            hidden_dim_2=best_params["hidden_dim_2"],
            dropout=best_params["dropout"],
        )
        model.load_state_dict(checkpoint["model_state"])
        model.eval()
        return model
    raise ValueError(f"Unsupported model name: {model_name}")


def get_best_model_bundle() -> dict:
    model_comparison = load_model_comparison()
    best_model_entry = get_best_entry(model_comparison)
    null_model_entry = get_null_model_entry(model_comparison)

    return {
        "model_comparison": model_comparison,
        "best_model_entry": best_model_entry,
        "null_model_entry": null_model_entry,
        "best_model": load_trained_model(best_model_entry["model"]),
    }


def print_best_model_summary(bundle: dict):
    best_model_entry = bundle["best_model_entry"]
    null_model_entry = bundle["null_model_entry"]
    print(f"Best Technology model: {best_model_entry['model']}")
    print(f"Best model test accuracy: {best_model_entry['test_accuracy']:.6f}")
    print(f"Null model test accuracy: {null_model_entry['test_accuracy']:.6f}")


best_model_TECH = None


def main():
    global best_model_TECH
    bundle = get_best_model_bundle()
    best_model_TECH = bundle["best_model"]
    print_best_model_summary(bundle)


if __name__ == "__main__":
    main()
