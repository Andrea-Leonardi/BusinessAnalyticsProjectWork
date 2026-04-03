import pandas as pd
import requests
import time
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from collections import deque
import glob
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 

# Rate Limiter per 200 richieste/minuto E 10 richieste/secondo
class RateLimiter:
    def __init__(self, max_requests_per_minute=200, max_requests_per_second=10):
        self.max_requests_per_minute = max_requests_per_minute
        self.max_requests_per_second = max_requests_per_second
        self.minute_window = 60
        self.second_window = 1.0
        self.requests_times = deque()
        self.last_request_time = 0
        self.min_interval = 1.0 / max_requests_per_second

    def wait_if_needed(self):
        now = time.time()
        time_since_last = now - self.last_request_time
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            time.sleep(sleep_time)
            now = time.time()
        while self.requests_times and self.requests_times[0] < now - self.minute_window:
            self.requests_times.popleft()
        if len(self.requests_times) >= self.max_requests_per_minute:
            sleep_time = self.minute_window - (now - self.requests_times[0]) + 0.1
            if sleep_time > 0:
                print(f"⏳ Pausa rate limit: {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                self.requests_times.clear()
            now = time.time()
        self.requests_times.append(now)
        self.last_request_time = now

# 1. API Configuration
API_KEY = "PKVESCM6H235I3XWBT25AYP7JC"
SECRET_KEY = "6MPjbdQyG6PmWP1niRAAGPFM3HQw2SvAWMGWL3r5Q3FM"
NEWS_URL = "https://data.alpaca.markets/v1beta1/news"
HEADERS = {
    "Apca-Api-Key-Id": API_KEY,
    "Apca-Api-Secret-Key": SECRET_KEY,
    "accept": "application/json"
}

limiter = RateLimiter()

# 2. Ticker Universe
try:
    data = pd.read_csv(cfg.ENT)
    tickers = data['Ticker'].unique().tolist()[80:110]
except FileNotFoundError:
    tickers = ['AAPL', 'MSFT', 'AMZN', 'GOOG', 'META']
    print("⚠️ File csv non trovato, uso test predefiniti.")

# 3. Funzione di generazione date
def generate_daily_date_pairs(trimesters):
    date_pairs = []
    for start_str, end_str in trimesters:
        start = datetime.strptime(start_str, "%Y-%m-%d")
        end = datetime.strptime(end_str, "%Y-%m-%d")
        current = start
        while current <= end:
            pair_end = current + timedelta(days=1)
            date_pairs.append([current.strftime("%Y-%m-%d"), pair_end.strftime("%Y-%m-%d")])
            current = pair_end
    return date_pairs

lista_date = [["2021-01-01", "2026-03-27"]]
lista_giornaliera = generate_daily_date_pairs(lista_date)

# 4. Loop di scaricamento
output_dir = 'raw_news_data'
os.makedirs(output_dir, exist_ok=True)

for ticker_idx, ticker in enumerate(tickers, 1):
    ticker_news = []
    print(f"📡 [{ticker_idx}/{len(tickers)}] Download per {ticker} ({len(lista_giornaliera)} giorni)... ")
    file_path = f"{output_dir}/{ticker}.parquet"

    if os.path.exists(file_path):
        print(f"  ⏩ {ticker} già scaricato, salto.")
        continue

    for start, end in lista_giornaliera:
        params = {"symbols": ticker, "start": start, "end": end, "limit": 50}
        limiter.wait_if_needed()
        try:
            res = requests.get(NEWS_URL, headers=HEADERS, params=params, timeout=10)
            if res.status_code == 200:
                arts = res.json().get('news', [])
                for a in arts:
                    ticker_news.append({
                        "ID": a.get('id'),
                        "Ticker": ticker,
                        "Date": a['created_at'],
                        "Source": a.get('source', ''),
                        "Headline": a['headline'],
                        "Summary": a.get('summary', ''),
                        "Content": a.get('content', ''),
                        "URL": a.get('url', '')
                    })
            else:
                print(f"  ❌ Errore API {res.status_code} il {start}")
        except Exception as e:
            print(f"  ❌ Errore connessione il {start}: {e}")

    if ticker_news:
        pd.DataFrame(ticker_news).to_parquet(file_path)
        print(f"  ✅ Salvati {len(ticker_news)} articoli per {ticker}.")

# Unione finale
path = 'raw_news_data/*.parquet'
all_files = glob.glob(path)
if all_files:
    print(f"📂 Unione di {len(all_files)} file in corso...")
    full_df = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)
    output_filename = 'news_dataset_full_text.csv'
    full_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
    print(f"✅ Dataset creato: {output_filename}")
    data.sort_values(by=["enterprise", "date"], ascending=[False, True], inplace=True)
    data.to_csv(cfg.NEWS_ARTICLES, index=False, encoding='utf-8-sig')