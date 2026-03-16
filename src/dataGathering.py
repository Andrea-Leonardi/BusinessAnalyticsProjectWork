# %%
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import config as cfg
import requests
import re
import json
from lxml import html # needed for pd.read_html

#lets take a small subset of the companies to test the code
# Adjust path if needed (since config uses .parent for Jupyter)
try:
    df = pd.read_csv(cfg.ENT).head(10)
except FileNotFoundError:
    # Fallback: assume script is in src/, so project root is ../
    try:
        project_root = Path(__file__).parent.parent
        ent_path = project_root / "data" / "possible_enterprises" / "enterprises.csv"
        df = pd.read_csv(ent_path).head(10)
    except NameError:
        # If __file__ not defined (e.g., in Jupyter), assume cwd is project root
        ent_path = Path.cwd() / "data" / "possible_enterprises" / "enterprises.csv"
        df = pd.read_csv(ent_path).head(10)


def get_revenue_from_macrotrends(ticker, company_name):
    """
    Scrapes revenue data from macrotrends.net for a given ticker and company name.
    It first tries to use pd.read_html to find the data table.
    If that fails, it falls back to a regex search on the script content.
    """
    # Improved slug generation
    name_for_slug = company_name.split(' ')[0]
    company_name_slug = name_for_slug.lower().replace('.', '')
    
    url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{company_name_slug}/revenue"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page for {ticker}: {e}")
        return None

    # Try parsing with pd.read_html first
    try:
        dfs = pd.read_html(response.text)
        revenue_df = None
        for df_table in dfs:
            if 'Year' in df_table.columns and any('Revenue' in str(col) for col in df_table.columns):
                revenue_col = [col for col in df_table.columns if 'Revenue' in str(col)][0]
                revenue_df = df_table[['Year', revenue_col]].copy()
                revenue_df.rename(columns={revenue_col: 'Revenue'}, inplace=True)
                break
        
        if revenue_df is not None:
            print(f"Successfully scraped revenue for {ticker} using pd.read_html.")
            revenue_df['Year'] = pd.to_datetime(revenue_df['Year'], format='%Y')
            revenue_df['Revenue'] = revenue_df['Revenue'].astype(str).str.replace(r'[\$,]', '', regex=True).astype(float)
            return revenue_df.dropna()

    except Exception as e:
        print(f"pd.read_html failed for {ticker}: {e}. Falling back to regex.")

    # Fallback to regex method if table not found or parsed
    match = re.search(r'var originalData = (.*?);', response.text)

    if not match:
        print(f"Could not find revenue data for {ticker} with regex fallback.")
        return None

    try:
        print(f"Trying regex fallback for {ticker}.")
        json_data = json.loads(match.group(1))
        if not json_data:
            return None
            
        revenue_df = pd.DataFrame(json_data)
        revenue_df['date'] = pd.to_datetime(revenue_df['date'])
        revenue_df.rename(columns={'date': 'Year', 'revenue': 'Revenue'}, inplace=True)
        revenue_df['Revenue'] = pd.to_numeric(revenue_df['Revenue'].astype(str).str.replace(',', ''), errors='coerce')
        return revenue_df
        
    except (json.JSONDecodeError, TypeError) as e:
        print(f"Error parsing data for {ticker} with regex fallback: {e}")
        return None

# %%
# Define the period for which to download data (last 5 years)
end_date = datetime.now()
start_date = pd.Timestamp(end_date - timedelta(days=365*5))

# create a dictionary to store the dataframes for each company
company_dfs = {}

for index, row in df.iterrows():
    ticker = row['Ticker']
    # Check if 'Company_name' column exists, otherwise use a placeholder
    if 'Company_name' in row:
        company_name = row['Company_name']
    else:
        # Fallback if 'Company_name' is not in the dataframe
        company_name = ticker

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
    
    # 2. Scrape revenue data
    revenue_df = get_revenue_from_macrotrends(ticker, company_name)
    
    if revenue_df is not None:
        print(f"Successfully scraped revenue for {ticker}")
        revenue_df.set_index('Year', inplace=True)
        # resample to monthly and ffill
        revenue_df_monthly = revenue_df.resample('M').ffill()
        
        # Merge with company_df
        company_df = company_df.merge(revenue_df_monthly, left_index=True, right_index=True, how='left')
        
    company_dfs[ticker] = company_df
    print(f"Data for {ticker}: {len(company_df)} months, {len(company_df.columns)} attributes")


# Now company_dfs contains a DataFrame for each ticker with the adjusted close prices
# Example access data

# %%
if 'AVGO' in company_dfs:
    example = company_dfs['AVGO']
    
# %%
#plot the data for each company different colors for each company standardize the data to make it comparable (divide by the first value) and plot the adjusted close prices over time
import matplotlib.pyplot as plt
plt.figure(figsize=(12, 6))
for ticker, df_plot in company_dfs.items():
    if 'Adj Close' in df_plot.columns and not df_plot['Adj Close'].empty:
        standardized_prices = df_plot['Adj Close'] / df_plot['Adj Close'].iloc[0]  # Standardize by the first value
        plt.plot(standardized_prices.index, standardized_prices.values, label=ticker)   
plt.title('Standardized Adjusted Close Prices Over Time')
plt.xlabel('Date')
plt.ylabel('Standardized Price')
plt.legend()
plt.grid()
plt.show()

# %%
# plot revenue for companies that have it
plt.figure(figsize=(12, 6))
for ticker, df_plot in company_dfs.items():
    if 'Revenue' in df_plot.columns and not df_plot['Revenue'].dropna().empty:
        plt.plot(df_plot.index, df_plot['Revenue'].values, label=f"{ticker} Revenue")   
plt.title('Revenue Over Time')
plt.xlabel('Date')
plt.ylabel('Revenue (Millions of US $)')
plt.legend()
plt.grid()
plt.show()
