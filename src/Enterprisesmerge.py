# %%
import pandas as pd
from pathlib import Path

# =========================================================
# Project paths
# =========================================================

# Assumiamo che VS Code sia aperto sulla cartella root della repo
ROOT = Path.cwd()
FOLDER_PATH = ROOT / "data" / "possible_enterprises"

# =========================================================
# Check folder exists
# =========================================================

if not FOLDER_PATH.exists():
    raise FileNotFoundError(
        f"La cartella non esiste: {FOLDER_PATH}\n"
        "Apri VS Code sulla cartella principale della repository."
    )

if not FOLDER_PATH.is_dir():
    raise NotADirectoryError(f"Il percorso non è una cartella: {FOLDER_PATH}")

# =========================================================
# List CSV files
# =========================================================

csv_files = sorted(FOLDER_PATH.glob("*.csv"))

if not csv_files:
    raise FileNotFoundError(
        f"Nessun file CSV trovato nella cartella: {FOLDER_PATH}"
    )

print("File CSV trovati:")
for file_path in csv_files:
    print(f"- {file_path.name}")

# =========================================================
# Read all CSV files
# =========================================================

dfs = {}

for file_path in csv_files:
    df = pd.read_csv(file_path)
    df["source"] = file_path.name
    dfs[file_path.name] = df
    print(f"Caricato {file_path.name} con {len(df)} righe")

# =========================================================
# Merge all DataFrames
# =========================================================

df_all = pd.concat(dfs.values(), ignore_index=True)

required_cols = ["symbol", "company", "identifier", "source"]
missing_cols = [col for col in required_cols if col not in df_all.columns]

if missing_cols:
    raise KeyError(
        f"Mancano queste colonne nei CSV letti: {missing_cols}"
    )

df_merged = (
    df_all
    .groupby(["symbol", "company", "identifier"], as_index=False)
    .agg({"source": lambda x: ", ".join(sorted(set(x)))})
)

print(f"\nDataFrame finale con {len(df_merged)} righe univoche")

# %%
df_merged.head()
# %%
