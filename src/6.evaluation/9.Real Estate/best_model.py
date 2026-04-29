import csv
import json
from pathlib import Path

import joblib
import torch
import torch.nn as nn


CURRENT_DIR = Path(__file__).resolve().parent
SECTOR_NAME = CURRENT_DIR.name.split(".", 1)[1] if "." in CURRENT_DIR.name else CURRENT_DIR.name
SUMMARY_CSV_PATH = CURRENT_DIR / "best_model_summary.csv"
MODELING_DIR = CURRENT_DIR.parents[1] / "4.modeling" / "classic_ML_model" / "9.Real Estate"
MODEL_COMPARISON_PATH = MODELING_DIR / "orchestrator_results" / "model_comparison.json"
EXCLUDED_MODELS = {"null_model", "always_zero", "always_one"}
BENCHMARK_MODELS = ("null_model", "always_one", "always_zero")


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
        raise ValueError("No eligible models found for Real Estate.")
    return max(eligible_models, key=lambda item: item["test_accuracy"])


def get_model_entry(model_comparison: dict, model_name: str) -> dict:
    for item in model_comparison["ranking"]:
        if item["model"] == model_name:
            return item
    raise ValueError(f"{model_name} not found in model comparison results.")


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
    benchmark_entries = {
        model_name: get_model_entry(model_comparison, model_name)
        for model_name in BENCHMARK_MODELS
    }

    return {
        "model_comparison": model_comparison,
        "best_model_entry": best_model_entry,
        "benchmark_entries": benchmark_entries,
        "best_model": load_trained_model(best_model_entry["model"]),
    }


def get_best_model_summary_row(model_comparison: dict | None = None) -> dict:
    if model_comparison is None:
        model_comparison = load_model_comparison()
    best_model_entry = get_best_entry(model_comparison)
    benchmark_entries = {
        model_name: get_model_entry(model_comparison, model_name)
        for model_name in BENCHMARK_MODELS
    }

    row = {
        "sector": SECTOR_NAME,
        "best_model": best_model_entry["model"],
        "test_accuracy": best_model_entry["test_accuracy"],
    }
    for benchmark_name in BENCHMARK_MODELS:
        benchmark_entry = benchmark_entries[benchmark_name]
        row[f"delta_{benchmark_name}"] = (
            best_model_entry["test_accuracy"] - benchmark_entry["test_accuracy"]
        )
    return row


def write_best_model_summary_csv(output_path: Path = SUMMARY_CSV_PATH) -> Path:
    row = get_best_model_summary_row()
    fieldnames = [
        "sector",
        "best_model",
        "test_accuracy",
        "delta_null_model",
        "delta_always_one",
        "delta_always_zero",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                field: (
                    f"{row[field]:.6f}"
                    if field not in {"sector", "best_model"}
                    else row[field]
                )
                for field in fieldnames
            }
        )
    return output_path

def print_best_model_summary(bundle: dict):
    best_model_entry = bundle["best_model_entry"]
    benchmark_entries = bundle["benchmark_entries"]
    print(f"Best {SECTOR_NAME} model: {best_model_entry['model']}")
    print(f"Best model test accuracy: {best_model_entry['test_accuracy']:.6f}")
    print("Benchmark comparison:")
    for benchmark_name in BENCHMARK_MODELS:
        benchmark_entry = benchmark_entries[benchmark_name]
        accuracy_gap = best_model_entry["test_accuracy"] - benchmark_entry["test_accuracy"]
        print(
            f"- {benchmark_name}: {benchmark_entry['test_accuracy']:.6f} "
            f"(delta vs best: {accuracy_gap:+.6f})"
        )


best_model_RE = None


def main():
    global best_model_RE
    bundle = get_best_model_bundle()
    best_model_RE = bundle["best_model"]
    print_best_model_summary(bundle)

    summary_path = write_best_model_summary_csv()

    print(f"Best model summary CSV saved to: {summary_path}")


if __name__ == "__main__":
    main()

