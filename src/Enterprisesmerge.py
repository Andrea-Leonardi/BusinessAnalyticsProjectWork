import pandas as pd
from pathlib import Path
import yfinance as yf
from tqdm import tqdm
import time

# =========================================================
# Project paths
# =========================================================

ROOT = Path.cwd()
FOLDER_PATH = ROOT / "data" / "possible_enterprises" / "raw"
OUTPUT_PATH = ROOT / "data" / "possible_enterprises" / "merged_enterprises.csv"
TOP_200_PATH = ROOT / "data" / "possible_enterprises" / "top_200_enterprises.csv"

# =========================================================
# Check folder exists
# =========================================================

if not FOLDER_PATH.exists():
    raise FileNotFoundError(f"La cartella non esiste: {FOLDER_PATH}")

if not FOLDER_PATH.is_dir():
    raise NotADirectoryError(f"Il percorso non è una cartella: {FOLDER_PATH}")

# =========================================================
# List and read CSV files
# =========================================================

csv_files = sorted(FOLDER_PATH.glob("*.csv"))
if not csv_files:
    raise FileNotFoundError(f"Nessun file CSV trovato in: {FOLDER_PATH}")

print("File CSV trovati:")
for file_path in csv_files:
    print(f"- {file_path.name}")

dfs = {}
for file_path in csv_files:
    df = pd.read_csv(file_path)
    df["source"] = file_path.name
    dfs[file_path.name] = df
    print(f"Caricato {file_path.name} con {len(df)} righe")

# =========================================================
# Merge DataFrames
# =========================================================

df_all = pd.concat(dfs.values(), ignore_index=True)
required_cols = ["symbol", "company", "identifier", "source"]
missing_cols = [col for col in required_cols if col not in df_all.columns]
if missing_cols:
    raise KeyError(f"Colonne mancanti: {missing_cols}")

df_merged = (
    df_all
    .groupby(["symbol", "company", "identifier"], as_index=False)
    .agg({"source": lambda x: ", ".join(sorted(set(x)))})
)
print(f"DataFrame merged con {len(df_merged)} righe univoche")

# =========================================================
# Clean data
# =========================================================

df_merged = df_merged[df_merged["symbol"] != "-"]
df_merged = df_merged.dropna(subset=["symbol", "company", "identifier"], how="all")
df_merged.rename(columns={"symbol": "Ticker", "company": "Company_name", "identifier": "CUSIP"}, inplace=True)
df_merged.reset_index(drop=True, inplace=True)

# Check for duplicates
duplicates = df_merged[df_merged.duplicated('Ticker', keep=False)]
if not duplicates.empty:
    print(f"Ticker duplicati trovati: {len(duplicates)} righe")
    df_unique = df_merged.drop_duplicates('Ticker')
    print(f"Dopo rimozione duplicati: {len(df_unique)} ticker unici")
else:
    df_unique = df_merged
    print("Tutti i ticker sono unici")

# =========================================================
# Fetch market caps (disabilitato, processo molto lento, utilizzare solo se necessario)
# =========================================================
if False:
    market_caps = {}
    errors = []
    for ticker in tqdm(df_unique['Ticker'], desc="Downloading market caps"):
        try:
            info = yf.Ticker(ticker).info
            market_caps[ticker] = info.get('marketCap')
            time.sleep(0.01)
        except Exception as e:
            market_caps[ticker] = None
            errors.append((ticker, str(e)))

    df_unique['Market_Cap'] = df_unique['Ticker'].map(market_caps)

    if errors:
        print(f"Errori per {len(errors)} ticker")
        for ticker, error in errors[:5]:
            print(f"{ticker}: {error}")

    print(f"Market cap recuperati: {len(df_unique) - len(errors)} / {len(df_unique)}")

    # =========================================================
    # Standardize tickers
    # =========================================================

    ticker_aliases = {
        'GOOG': 'GOOGL',
        'BRK.B': 'BRK.A',
    }

    df_unique['Standard_Ticker'] = df_unique['Ticker'].replace(ticker_aliases)
    df_final = df_unique.groupby('Standard_Ticker', as_index=False).first()
    df_final.rename(columns={'Standard_Ticker': 'Ticker'}, inplace=True)
    print(f"Dopo standardizzazione: {len(df_final)} aziende uniche")
    df_unique = df_final

    # =========================================================
    # Final clean and save
    # =========================================================

    df_unique = df_unique.dropna(subset=['Market_Cap'])
    df_unique.to_csv(OUTPUT_PATH, index=False)
    print(f"Salvato merged in: {OUTPUT_PATH}")

# =========================================================
# Load, sort, get top 200
# =========================================================

df_unique = pd.read_csv(OUTPUT_PATH)
df_unique.sort_values(by="Market_Cap", ascending=False, inplace=True)
df_unique.reset_index(drop=True, inplace=True)
top_200 = df_unique.head(200)

# Fetch sectors
sectors = {}
for ticker in tqdm(top_200['Ticker'], desc="Fetching sectors"):
    try:
        info = yf.Ticker(ticker).info
        sectors[ticker] = info.get('sector')
        time.sleep(0.01)
    except Exception as e:
        sectors[ticker] = None

top_200['Sector'] = top_200['Ticker'].map(sectors)
top_200.to_csv(TOP_200_PATH, index=False)
print(f"Salvato top 200 in: {TOP_200_PATH}")



