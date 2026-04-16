import csv
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent / "classic_ML_model"
RESULTS_DIR = BASE_DIR / "orchestrator_results"

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import split_data as split_data_module

# Set to False to exclude a model from the run.
INCLUDE_NULL_MODEL = True
INCLUDE_ALWAYS_ZERO = True
INCLUDE_ALWAYS_ONE = True
INCLUDE_LASSO_LOGISTIC = True
INCLUDE_LOGISTIC_REGRESSION = True
INCLUDE_RANDOM_FOREST = True
INCLUDE_XGBOOST = True
INCLUDE_NEURAL_NETWORK = True

MODEL_RUNS = [
    {
        "name": "null_model",
        "enabled": INCLUDE_NULL_MODEL,
        "directory": BASE_DIR / "null_model",
        "steps": ["training_model.py", "performance.py"],
    },
    {
        "name": "always_zero",
        "enabled": INCLUDE_ALWAYS_ZERO,
        "directory": BASE_DIR / "always_zero_model",
        "steps": ["performance.py"],
    },
    {
        "name": "always_one",
        "enabled": INCLUDE_ALWAYS_ONE,
        "directory": BASE_DIR / "always_one_model",
        "steps": ["performance.py"],
    },
    {
        "name": "lasso_logistic",
        "enabled": INCLUDE_LASSO_LOGISTIC,
        "directory": BASE_DIR / "lasso_model",
        "steps": ["validation.py", "training_model.py", "variable_selection.py", "performance.py"],
    },
    {
        "name": "logistic_regression",
        "enabled": INCLUDE_LOGISTIC_REGRESSION,
        "directory": BASE_DIR / "logistic_regression",
        "steps": ["training_model.py", "performance.py"],
    },
    {
        "name": "random_forest",
        "enabled": INCLUDE_RANDOM_FOREST,
        "directory": BASE_DIR / "random_forest",
        "steps": ["validation.py", "training_model.py", "performance.py"],
    },
    {
        "name": "xgboost",
        "enabled": INCLUDE_XGBOOST,
        "directory": BASE_DIR / "XGBoost",
        "steps": ["validation.py", "training_model.py", "performance.py"],
    },
    {
        "name": "neural_network",
        "enabled": INCLUDE_NEURAL_NETWORK,
        "directory": BASE_DIR / "neural network",
        "steps": ["validation.py", "training_model.py", "performance.py"],
    },
]


def run_python_script(script_path: Path):
    relative_path = script_path.relative_to(BASE_DIR.parent)
    print(f"\n=== Running {relative_path} ===")
    subprocess.run(
        [sys.executable, str(script_path)],
        check=True,
        cwd=script_path.parent,
    )


def read_performance_file(model_directory: Path) -> dict:
    performance_path = model_directory / "performance.json"
    with open(performance_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_dataset_sizes() -> dict:
    return {
        "training": len(split_data_module.X_train),
        "validation": len(split_data_module.X_validation),
        "test": len(split_data_module.X_test),
    }


def save_summary(results: list[dict]):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    ranking = sorted(results, key=lambda item: item["test_accuracy"], reverse=True)
    best_accuracy = ranking[0]["test_accuracy"]
    tied_best_models = [item["model"] for item in ranking if item["test_accuracy"] == best_accuracy]
    dataset_sizes = get_dataset_sizes()

    summary = {
        "metric": "accuracy",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_sizes": dataset_sizes,
        "execution_order": [item["model"] for item in results],
        "ranking": ranking,
        "best_model": ranking[0]["model"],
        "best_accuracy": best_accuracy,
        "tied_best_models": tied_best_models,
    }

    with open(RESULTS_DIR / "model_comparison.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4)

    with open(RESULTS_DIR / "model_comparison.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["model", "validation_accuracy", "test_accuracy"],
        )
        writer.writeheader()
        for item in ranking:
            writer.writerow(
                {
                    "model": item["model"],
                    "validation_accuracy": item.get("validation_accuracy"),
                    "test_accuracy": item["test_accuracy"],
                }
            )

    return summary


def print_summary(summary: dict):
    dataset_sizes = summary["dataset_sizes"]
    print(
        "\nDataset sizes | "
        f"training={dataset_sizes['training']} | "
        f"validation={dataset_sizes['validation']} | "
        f"test={dataset_sizes['test']}"
    )
    print("\n=== Accuracy Summary ===")
    for index, item in enumerate(summary["ranking"], start=1):
        validation_accuracy = item.get("validation_accuracy")
        validation_label = (
            f"{validation_accuracy:.6f}" if validation_accuracy is not None else "-"
        )
        print(
            f"{index}. {item['model']} | "
            f"validation_accuracy={validation_label} | "
            f"test_accuracy={item['test_accuracy']:.6f}"
        )

    if len(summary["tied_best_models"]) == 1:
        print(
            f"\nBest model by accuracy: {summary['best_model']} "
            f"({summary['best_accuracy']:.6f})"
        )
    else:
        tied_models = ", ".join(summary["tied_best_models"])
        print(
            f"\nBest accuracy tie at {summary['best_accuracy']:.6f}: {tied_models}"
        )

    print(f"Summary saved in: {RESULTS_DIR}")


def main():
    results = []
    enabled_model_runs = [model_run for model_run in MODEL_RUNS if model_run["enabled"]]

    if not enabled_model_runs:
        raise ValueError("No models enabled. Set at least one INCLUDE_* flag to True.")

    for model_run in enabled_model_runs:
        for step_name in model_run["steps"]:
            run_python_script(model_run["directory"] / step_name)
        results.append(read_performance_file(model_run["directory"]))

    summary = save_summary(results)
    print_summary(summary)


if __name__ == "__main__":
    main()
