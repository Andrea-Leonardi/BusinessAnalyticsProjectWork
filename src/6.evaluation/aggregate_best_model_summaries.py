import argparse
import csv
import json
import math
from pathlib import Path


CURRENT_DIR = Path(__file__).resolve().parent
SUMMARY_FILENAME = "best_model_summary.csv"
DEFAULT_OUTPUT_PATH = CURRENT_DIR / "best_models_summary.csv"
MODELING_DIR = CURRENT_DIR.parent / "4.modeling" / "classic_ML_model"
CONFIDENCE_Z_95 = 1.96
FIELDNAMES = [
    "sector",
    "best_model",
    "test_accuracy",
    "test_accuracy_ci_95",
    "delta_null_model",
    "delta_always_one",
    "delta_always_zero",
]


def sector_sort_key(path: Path) -> tuple[int, str]:
    prefix, _, name = path.name.partition(".")
    if prefix.isdigit():
        return int(prefix), name
    return 999, path.name


def iter_sector_dirs() -> list[Path]:
    return sorted(
        [
            path
            for path in CURRENT_DIR.iterdir()
            if path.is_dir() and not path.name.startswith("__")
        ],
        key=sector_sort_key,
    )


def read_sector_summary(summary_path: Path) -> dict:
    with open(summary_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if len(rows) != 1:
        raise ValueError(f"{summary_path} must contain exactly one data row.")
    return rows[0]


def read_sector_test_size(sector_dir: Path) -> int:
    model_comparison_path = (
        MODELING_DIR
        / sector_dir.name
        / "orchestrator_results"
        / "model_comparison.json"
    )
    with open(model_comparison_path, "r", encoding="utf-8") as f:
        model_comparison = json.load(f)
    test_size = model_comparison["dataset_sizes"]["test"]
    if test_size <= 0:
        raise ValueError(f"{model_comparison_path} has an invalid test size: {test_size}")
    return test_size


def format_test_accuracy_ci_95(test_accuracy: float, test_size: int) -> str:
    margin = CONFIDENCE_Z_95 * math.sqrt(
        test_accuracy * (1 - test_accuracy) / test_size
    )
    lower_bound = max(0, test_accuracy - margin)
    upper_bound = min(1, test_accuracy + margin)
    return f"[{lower_bound:.6f}, {upper_bound:.6f}]"


def add_test_accuracy_ci(row: dict, sector_dir: Path) -> dict:
    enriched_row = row.copy()
    test_accuracy = float(enriched_row["test_accuracy"])
    test_size = read_sector_test_size(sector_dir)
    enriched_row["test_accuracy_ci_95"] = format_test_accuracy_ci_95(
        test_accuracy,
        test_size,
    )
    return enriched_row


def collect_summaries(strict: bool = False) -> tuple[list[dict], list[Path]]:
    rows = []
    missing_paths = []

    for sector_dir in iter_sector_dirs():
        summary_path = sector_dir / SUMMARY_FILENAME
        if not summary_path.exists():
            missing_paths.append(summary_path)
            continue
        rows.append(add_test_accuracy_ci(read_sector_summary(summary_path), sector_dir))

    if strict and missing_paths:
        missing = "\n".join(str(path) for path in missing_paths)
        raise FileNotFoundError(f"Missing sector summary CSV files:\n{missing}")

    return rows, missing_paths


def write_aggregate_csv(output_path: Path, rows: list[dict]) -> Path:
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate per-sector best_model_summary.csv files."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output CSV path. Default: {DEFAULT_OUTPUT_PATH}",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if one or more sector summary CSV files are missing.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rows, missing_paths = collect_summaries(strict=args.strict)
    if not rows:
        raise FileNotFoundError(
            "No best_model_summary.csv files found. Run each sector best_model.py first."
        )

    output_path = write_aggregate_csv(args.output, rows)
    print(f"Aggregate best model summary saved to: {output_path}")
    if missing_paths:
        print("Skipped sectors without best_model_summary.csv:")
        for missing_path in missing_paths:
            print(f"- {missing_path.parent.name}")


if __name__ == "__main__":
    main()
