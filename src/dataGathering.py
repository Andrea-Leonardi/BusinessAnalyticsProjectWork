# %%
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import config as cfg

#lets take a small subset of the companies to test the code
# Adjust path if needed (since config uses .parent for Jupyter)
try:
    df = pd.read_csv(cfg.ENT).head(10)
except FileNotFoundError:
    # Fallback: assume script is in src/, so project root is ../
    try:
        project_root = Path(__file__).parent.parent
        ent_path = project_root / "data" / "possible_enterprises" / "enterprises.csv"
        df = pd.read_csv(ent_path).head(10).drop(columns=["source"])
    except NameError:
        # If __file__ not defined (e.g., in Jupyter), assume cwd is project root
        ent_path = Path.cwd() / "data" / "possible_enterprises" / "enterprises.csv"
        df = pd.read_csv(ent_path).head(10).drop(columns=["source"]) 




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

    company_dfs[ticker] = company_df
    print(f"Data for {ticker}: {len(company_df)} months, {len(company_df.columns)} attributes")

# Now company_dfs contains a DataFrame for each ticker with the adjusted close prices
# Example access data

# %%
example = company_dfs['AVGO']
    
# %%
#plot the data for each company different colors for each company standardize the data to make it comparable (divide by the first value) and plot the adjusted close prices over time
import matplotlib.pyplot as plt
plt.figure(figsize=(12, 6))
for ticker, df in company_dfs.items():
    standardized_prices = df['Adj Close'] / df['Adj Close'].iloc[0]  # Standardize by the first value
    plt.plot(standardized_prices.index, standardized_prices.values, label=ticker)   
plt.title('Standardized Adjusted Close Prices Over Time')
plt.xlabel('Date')
plt.ylabel('Standardized Price')
plt.legend()
plt.grid()
plt.show()


# %%
