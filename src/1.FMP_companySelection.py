#%%
import pandas as pd
import requests
import config as cfg


# Simple example of how to call the FMP stock screener API.
# The goal here is to keep the script easy to read and easy to modify.

url = "https://financialmodelingprep.com/stable/company-screener"
api_key = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"

# Change these parameters to try different filters.
# You can remove a parameter if you do not want to use it.
params = {
    "apikey": api_key,
    "isEtf": False,
    "isFund": False,
    "isActivelyTrading": True,
    "limit": 500,
    "includeAllShareClasses": False,
}

# Send the request to FMP.
response = requests.get(url, params=params, timeout=30)
response.raise_for_status()

# Convert the JSON response into a pandas DataFrame.
data = response.json()
df_raw = pd.DataFrame(data)


# %%

#ci sono aziende con più di un ticker, prendo solo quello con market cap più alto
df = df_raw.sort_values(by="marketCap", ascending=False).drop_duplicates(subset=["companyName"], keep="first")

#elimino GEV che non ha dati finanziari
df = df[df["symbol"] != "GEV"]
#elimino aziende che non provengono da Nasdaq, NYSE
df = df[df["exchangeShortName"].isin(["NASDAQ", "NYSE"])]

#prendo le prime 10 aziende con market cap più alto per ogni settore
df = df.sort_values(by="marketCap", ascending=False).groupby("sector").head(10)

#droppo tutte le colonne tranne symbol, companyName, sector, industry, marketCap
df = df[["symbol", "companyName", "sector", "industry", "marketCap"]]
#rinomino la colonna symbol in Ticker
df = df.rename(columns={"symbol": "Ticker"})

# Save the result directly inside data.
df.to_csv(cfg.ENT, index=False)
print(f"Saved file: {cfg.ENT}")

# %%
