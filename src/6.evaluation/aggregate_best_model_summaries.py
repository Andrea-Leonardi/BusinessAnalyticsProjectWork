import argparse
import csv
from pathlib import Path


CURRENT_DIR = Path(__file__).resolve().parent
SUMMARY_FILENAME = "best_model_summary.csv"
DEFAULT_OUTPUT_PATH = CURRENT_DIR / "best_models_summary.csv"
FIELDNAMES = [
    "sector",
    "best_model",
    "test_accuracy",
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


def collect_summaries(strict: bool = False) -> tuple[list[dict], list[Path]]:
    rows = []
    missing_paths = []

    for sector_dir in iter_sector_dirs():
        summary_path = sector_dir / SUMMARY_FILENAME
        if not summary_path.exists():
            missing_paths.append(summary_path)
            continue
        rows.append(read_sector_summary(summary_path))

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
