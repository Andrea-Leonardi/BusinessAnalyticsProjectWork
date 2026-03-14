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

# =========================================================
# drop all companies with symbol = "-"
# =========================================================

df_merged = df_merged[df_merged["symbol"] != "-"]
df_merged = df_merged.dropna(subset=["symbol", "company", "identifier"], how="all")
df_merged.rename(columns={"symbol": "Ticker", "company": "Company_name", "identifier": "CUSIP"}, inplace=True)
df_merged.reset_index(drop=True, inplace=True)

# =========================================================
# Check for duplicate tickers
# =========================================================

duplicates = df_merged[df_merged.duplicated('Ticker', keep=False)]
if not duplicates.empty:
    print(f"Ci sono {len(duplicates)} righe con ticker duplicati:")
    print(duplicates[['Ticker', 'Company_name', 'CUSIP', 'source']].head(10))
    # Per gestire, puoi scegliere di tenere solo la prima occorrenza per ticker
    df_unique = df_merged.drop_duplicates('Ticker')
    print(f"\nDopo aver rimosso duplicati, rimangono {len(df_unique)} ticker unici.")
else:
    df_unique = df_merged
    print("Tutti i ticker sono unici.")




# %%
df_unique.head()
# %%
import yfinance as yf
from tqdm import tqdm
import time

# =========================================================
# Fetch market caps for unique tickers with progress bar and error handling
# =========================================================

market_caps = {}
errors = []

for ticker in tqdm(df_unique['Ticker'], desc="Downloading market caps"):
    try:
        info = yf.Ticker(ticker).info
        market_caps[ticker] = info.get('marketCap')
        # Aggiungi un piccolo delay per evitare rate limiting
        time.sleep(0.01)
    except Exception as e:
        market_caps[ticker] = None
        errors.append((ticker, str(e)))

# =========================================================
# Add market caps to df_unique
# =========================================================

df_unique['Market_Cap'] = df_unique['Ticker'].map(market_caps)

# =========================================================
# Report errors
# =========================================================

if errors:
    print(f"\nErrori per {len(errors)} ticker:")
    for ticker, error in errors[:10]:  # Mostra solo i primi 10
        print(f"{ticker}: {error}")
    if len(errors) > 10:
        print(f"... e altri {len(errors) - 10}")

print(f"\nMarket cap recuperati per {len(df_unique) - len(errors)} su {len(df_unique)} ticker")

# %%
df_unique = df_unique.dropna(subset=['Market_Cap'])
#salva il DataFrame finale in un nuovo CSV
output_path = ROOT / "data" / "possible_enterprises" / "merged_enterprises.csv"
df_unique.to_csv(output_path, index=False)
# %%
df_unique = pd.read_csv(output_path)
df_unique.sort_values(by="Market_Cap", ascending=False, inplace=True)
df_unique.reset_index(drop=True, inplace=True)
# %%
#tengo le prime 200 aziende per market cap
top_200 = df_unique.head(200)   
print(yf.ticker("aapl").info)

# %%
