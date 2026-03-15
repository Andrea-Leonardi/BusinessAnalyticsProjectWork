# %%
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import config as cfg

#lets take a small subset of the companies to test the code
# Adjust path if needed (since config uses .parent for Jupyter)
try:
    df = pd.read_csv(cfg.TOP200).head(10).drop(columns=["source"])
except FileNotFoundError:
    # Fallback: assume script is in src/, so project root is ../
    try:
        project_root = Path(__file__).parent.parent
        top200_path = project_root / "data" / "possible_enterprises" / "top_200_enterprises.csv"
        df = pd.read_csv(top200_path).head(10).drop(columns=["source"])
    except NameError:
        # If __file__ not defined (e.g., in Jupyter), assume cwd is project root
        top200_path = Path.cwd() / "data" / "possible_enterprises" / "top_200_enterprises.csv"
        df = pd.read_csv(top200_path).head(10).drop(columns=["source"]) 




# %%
# Define the period for which to download data (last 2 years)
end_date = datetime.now()
start_date = pd.Timestamp(end_date - timedelta(days=365*5))

# create a dictionary to store the dataframes for each company
company_dfs = {}

for ticker in df["Ticker"]:
    print(f"download data for {ticker}...")
    # download the historical data for the ticker (monthly data with auto_adjust)
    stock = yf.Ticker(ticker)
    data = stock.history(period='5y', interval='1mo', auto_adjust=True)
    if not data.empty:
        # 'Close' is the adjusted close price
        adj_close = data['Close']
        price_series = adj_close
        print(f"Using auto-adjusted monthly close prices for {ticker}")
        try:
            company_dfs[ticker] = pd.DataFrame({'Adj Close': price_series})
        except ValueError:
            # If price_series is scalar
            company_dfs[ticker] = pd.DataFrame({'Adj Close': [price_series]}, index=[pd.Timestamp.now()])
        company_dfs[ticker].index.name = 'Date'
        print(f"data for {ticker}: {len(company_dfs[ticker])} months")
    else:
        print(f"no data available for {ticker}")

# Now company_dfs contains a DataFrame for each ticker with the adjusted close prices
# Example access AAPL data
aapl = company_dfs['AAPL']
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
