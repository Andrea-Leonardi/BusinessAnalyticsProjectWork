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

    # 2. Get financial statements (annual data)
    financials = stock.financials
    balance_sheet = stock.balance_sheet

    # 3. Extract specific financial attributes and combine them
    financial_data = {}
    
    # Attributes from financials
    if not financials.empty:
        fin_attrs = ["Total Revenue", "Net Income", "Operating Income", "EBITDA", "Gross Profit"]
        for attr in fin_attrs:
            if attr in financials.index:
                financial_data[attr] = financials.loc[attr]

    # Attributes from balance sheet
    if not balance_sheet.empty:
        bs_attrs = ["Total Assets", "Stockholders Equity", "Total Debt", "Current Assets", "Current Liabilities"]
        for attr in bs_attrs:
            if attr in balance_sheet.index:
                financial_data[attr] = balance_sheet.loc[attr]

    # 4. Merge financial data with monthly stock prices
    if financial_data:
        fin_df = pd.DataFrame(financial_data)
        fin_df.index = pd.to_datetime(fin_df.index)
        
        # Use merge_asof to combine monthly prices with annual financials
        company_df = pd.merge_asof(
            company_df.sort_index(),
            fin_df.sort_index(),
            left_index=True,
            right_index=True,
            direction='backward' # Propagate last known financial value forward
        )

    # 5. Add shares outstanding (a single value)
    try:
        shares = stock.info.get('sharesOutstanding')
        if shares:
            company_df['Shares Outstanding'] = shares
    except Exception as e:
        print(f"Could not retrieve shares outstanding for {ticker}: {e}")

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
