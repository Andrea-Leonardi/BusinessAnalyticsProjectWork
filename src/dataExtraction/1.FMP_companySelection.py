#%%
import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config as cfg


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Define the FMP screener endpoint used to build the initial company universe.
url = "https://financialmodelingprep.com/stable/company-screener"
api_key = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"

# Set the screener filters that restrict the universe to active equities.
params = {
    "apikey": api_key,
    "isEtf": False,
    "isFund": False,
    "isActivelyTrading": True,
    "limit": 500,
    "includeAllShareClasses": False,
}


# ---------------------------------------------------------------------------
# Download Screener Results
# ---------------------------------------------------------------------------

# Request the filtered company universe from FMP.
response = requests.get(url, params=params, timeout=30)
response.raise_for_status()

# Convert the API response into a DataFrame for local filtering.
data = response.json()
df_raw = pd.DataFrame(data)


# %%


# ---------------------------------------------------------------------------
# Filter Company Universe
# ---------------------------------------------------------------------------

# Some companies appear with multiple tickers or share classes.
# Keep only the line with the highest market cap for each company name.
df = (
    df_raw.sort_values(by="marketCap", ascending=False)
    .drop_duplicates(subset=["companyName"], keep="first")
)

# Remove tickers that do not have the downstream data quality required by the
# current project pipeline.
excluded_tickers = [
    "GEV",
    "TBB",
    "RCB",
    "PLTR",
    "HSBC",
    "BAC",
    "JPM",
    "WFC",
    "MUFG",
]
df = df[~df["symbol"].isin(excluded_tickers)]

# Keep only companies listed on the two target US exchanges.
df = df[df["exchangeShortName"].isin(["NASDAQ", "NYSE"])]

# Keep the top 10 companies by market cap within each sector.
df = df.sort_values(by="marketCap", ascending=False).groupby("sector").head(10)

# Keep only the columns needed by the rest of the project.
df = df[["symbol", "companyName", "sector", "industry", "marketCap"]]

# Rename the ticker column to match the naming used in the other scripts.
df = df.rename(columns={"symbol": "Ticker"})


# ---------------------------------------------------------------------------
# Save Output
# ---------------------------------------------------------------------------

# Save the final company universe inside the shared data folder.
cfg.ENT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(cfg.ENT, index=False)
print(f"Saved file: {cfg.ENT}")

# %%
