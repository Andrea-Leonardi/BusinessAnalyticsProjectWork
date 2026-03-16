# %%
import sys
from pathlib import Path

sys.path.append(str(Path.cwd() / "src"))

import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import config as cfg
import requests
from lxml import html
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
#==========================================================
# Download historical price data for each company using yfinance
#==========================================================


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
print(df[["Ticker","Company_name"]])
# %%
#==========================================================
# Web scraping for revenue data from macrotrends.net without using Selenium
#==========================================================



# URL and XPath provided by the user
url = "https://www.macrotrends.net/stocks/charts/AAPL/apple/revenue"

# Set headers to mimic a browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}


# Fetch the page content
print(f"Fetching content from: {url}")
response = requests.get(url, headers=headers)
response.raise_for_status()  # Raise an exception for bad status codes

# Parse the HTML content
tree = html.fromstring(response.content)

for i in range(1, 11):
    for j in range(1, 3):
        xpath = f"//*[@id='main_content']/div[9]/div/div[1]/table/tbody/tr[{i}]/td[{j}]"
        elements = tree.xpath(xpath)
        if j == 1:
            year = elements[0].text_content()
        elif j == 2:
            revenue = elements[0].text_content()
            print(f"Year: {year}, Revenue: {revenue}")




# %%

#devo scaricare i nomi delle aziende usate da macrotrends 
import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

url = "https://www.macrotrends.net/stocks/stock-screener"

service = Service(ChromeDriverManager().install())

options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920x1080")

driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 15)

driver.get(url)

all_data = []

n_pages = 20   # cambia questo numero come vuoi

for page in range(1, n_pages + 1):
    print(f"Sto leggendo pagina {page}...")

    data = []
    
    for i in range(0, 20):   # meglio partire da 0
        try:
            ticker_xpath = f"//*[@id='row{i}jqxGrid']/div[2]/div"
            link_xpath = f"//*[@id='row{i}jqxGrid']/div[1]/div/div/a"

            ticker_element = wait.until(
                EC.presence_of_element_located((By.XPATH, ticker_xpath))
            )
            link_element = wait.until(
                EC.presence_of_element_located((By.XPATH, link_xpath))
            )

            ticker = ticker_element.text.strip()
            company = link_element.text.strip()
            link = link_element.get_attribute("href")

            data.append({
                "Ticker": ticker,
                "Company": company,
                "Link": link
            })

        except Exception as e:
            print(f"Could not extract row {i} on page {page}: {e}")

    all_data.extend(data)

    # se non siamo all'ultima pagina richiesta, vai avanti
    if page < n_pages:
        try:
            old_first_row = driver.find_element(By.ID, "row0jqxGrid").text

            next_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div[title='next']"))
            )
            driver.execute_script("arguments[0].click();", next_button)

            wait.until(
                lambda d: d.find_element(By.ID, "row0jqxGrid").text != old_first_row
            )

            time.sleep(1)

        except Exception as e:
            print(f"Non riesco a cambiare pagina: {e}")
            break

dfNames = pd.DataFrame(all_data).drop_duplicates()
print(dfNames)

driver.quit()

#salvo i dati in un csv
dfNames.to_csv(cfg.COMPANY_NAMES_CSV, index=False)

# %%
# Pulisco i link rimuovendo la parte "/stock-price-history" che non serve
dfNames["Link"] = dfNames["Link"].str.replace("/stock-price-history", "", regex=False)

# %%
#inserisco i link in df
df_merged = pd.merge(df, dfNames[["Ticker", "Link" ]], left_on="Ticker", right_on="Ticker", how="left")
# %%