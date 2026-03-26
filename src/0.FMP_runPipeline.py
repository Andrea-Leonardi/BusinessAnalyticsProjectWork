#%%
import subprocess
import sys

import config as cfg


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SINGLE_COMPANY_OUTPUT_DIRS = [
    cfg.SINGLE_COMPANY_PRICES,
    cfg.SINGLE_COMPANY_FINANCIALS,
    cfg.SINGLE_COMPANY_FULL_DATA,
]

PIPELINE_SCRIPTS = [
    cfg.SRC / "1.FMP_companySelection.py",
    cfg.SRC / "2.priceDataGathering.py",
    cfg.SRC / "3.FMP_financialsDataGathering.py",
    cfg.SRC / "4.FMP_financialsDataProcessing.py",
    cfg.SRC / "6.FMP_dataMerge.py",
]


# ---------------------------------------------------------------------------
# Clean Single-Company CSV Files
# ---------------------------------------------------------------------------

deleted_files = 0
for output_dir in SINGLE_COMPANY_OUTPUT_DIRS:
    output_dir.mkdir(parents=True, exist_ok=True)

    for csv_file in output_dir.glob("*.csv"):
        csv_file.unlink()
        deleted_files += 1

print(f"Deleted {deleted_files} single-company CSV files.")


# ---------------------------------------------------------------------------
# Run Full Pipeline
# ---------------------------------------------------------------------------

for script_path in PIPELINE_SCRIPTS:
    print(f"\nRunning {script_path.name}...")
    subprocess.run([sys.executable, str(script_path)], check=True)

print("\nPipeline completed successfully.")


# %%
