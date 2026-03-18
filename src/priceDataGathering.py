# %%
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import config as cfg

#lets take a small subset of the companies to test the code
df = pd.read_csv(cfg.ENT).head(10)

# %%
# Define the period for which to download data (last 5 years)
end_date = datetime.now()
start_date = pd.Timestamp(end_date - timedelta(days=365*5))

# create a dictionary to store the dataframes for each company
company_dfs = {}

for ticker in df["Ticker"]:
    print(f"download data for {ticker}...")
    stock = yf.Ticker(ticker)

    # 1. Download historical price monthly stock data
    hist_data = stock.history(period='5y', interval='1mo', auto_adjust=True)
    if hist_data.empty:
        print(f"No historical price data available for {ticker}")
        continue
    
    company_df = pd.DataFrame({'Adj Close': hist_data['Close']})
    company_df.index = company_df.index.tz_localize(None)
    company_df.index.name = 'Date'
    company_df["Ticker"] = ticker
    company_dfs[ticker] = company_df.reset_index()

if company_dfs:
    final_df = pd.concat(company_dfs.values(), ignore_index=True)
    final_df.to_csv(cfg.PRICE_DATA, index=False)
    print(f"Saved file: {cfg.PRICE_DATA}")
else:
    print("No price data was downloaded.")

# %%
