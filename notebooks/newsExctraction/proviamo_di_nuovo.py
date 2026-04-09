import pandas as pd
import requests
import time
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from collections import deque


# Rate Limiter per 200 richieste/minuto E 10 richieste/secondo
class RateLimiter:
    def __init__(self, max_requests_per_minute=200, max_requests_per_second=10):
        self.max_requests_per_minute = max_requests_per_minute
        self.max_requests_per_second = max_requests_per_second
        self.minute_window = 60
        self.second_window = 1.0
        self.requests_times = deque()
        self.last_request_time = 0
        self.min_interval = 1.0 / max_requests_per_second  # 0.1s per 10 req/s
    
    def wait_if_needed(self):
        """Attende se necessario per rispettare entrambi i rate limit"""
        now = time.time()
        
        # 1. Rate limit al secondo: assicura almeno min_interval tra richieste
        time_since_last = now - self.last_request_time
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            time.sleep(sleep_time)
            now = time.time()
        
        # 2. Rate limit al minuto: traccia le ultime 200 richieste
        # Rimuovi richieste fuori dalla finestra temporale di 60 secondi
        while self.requests_times and self.requests_times[0] < now - self.minute_window:
            self.requests_times.popleft()
        
        # Se abbiamo raggiunto il limite di 200/minuto, attendi
        if len(self.requests_times) >= self.max_requests_per_minute:
            sleep_time = self.minute_window - (now - self.requests_times[0]) + 0.1
            if sleep_time > 0:
                print(f"⏳ Limite minuto raggiunto (200/min). Pausa di {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                self.requests_times.clear()
            now = time.time()
        
        # Registra questa richiesta
        self.requests_times.append(now)
        self.last_request_time = now


# 1. API & Path Configuration
API_KEY = "PKVESCM6H235I3XWBT25AYP7JC"
SECRET_KEY = "6MPjbdQyG6PmWP1niRAAGPFM3HQw2SvAWMGWL3r5Q3FM"
NEWS_URL = "https://data.alpaca.markets/v1beta1/news"
HEADERS = {
    "Apca-Api-Key-Id": API_KEY,
    "Apca-Api-Secret-Key": SECRET_KEY,
    "accept": "application/json"
}

# Inizializza il rate limiter (200 richieste/minuto, 10 al secondo)
limiter = RateLimiter(max_requests_per_minute=200, max_requests_per_second=10)



# 2. Ticker Universe Ingestion
# Loading 110 tickers from a CSV configuration file
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg
data = pd.read_csv(cfg.ENT)
tickers = data['Ticker'].unique().tolist()

print(f"✅ Loaded {len(tickers)} tickers from universe.")




# 3. Creazione di una lista di date su base fiornaliera 
# Lista dei trimestri
lista_date = [
    ["2021-01-01", "2021-03-31"], ["2021-04-01", "2021-06-30"],
    ["2021-07-01", "2021-09-30"], ["2021-10-01", "2021-12-31"],
    ["2022-01-01", "2022-03-31"], ["2022-04-01", "2022-06-30"],
    ["2022-07-01", "2022-09-30"], ["2022-10-01", "2022-12-31"],
    ["2023-01-01", "2023-03-31"], ["2023-04-01", "2023-06-30"],
    ["2023-07-01", "2023-09-30"], ["2023-10-01", "2023-12-31"],
    ["2024-01-01", "2024-03-31"], ["2024-04-01", "2024-06-30"],
    ["2024-07-01", "2024-09-30"], ["2024-10-01", "2024-12-31"],
    ["2025-01-01", "2025-03-31"], ["2025-04-01", "2025-06-30"],
    ["2025-07-01", "2025-09-30"], ["2025-10-01", "2025-12-31"],
    ["2026-01-01", "2026-03-27"]
]

def generate_daily_date_pairs(trimesters):
    """Genera coppie di date consecutive non sovrapposte"""
    date_pairs = []
    for start_str, end_str in trimesters:
        start = datetime.strptime(start_str, "%Y-%m-%d")
        end = datetime.strptime(end_str, "%Y-%m-%d")
        current = start
        while current < end:
            pair_end = current + timedelta(days=1)
            if pair_end > end:
                pair_end = end
            date_pairs.append([current.strftime("%Y-%m-%d"), pair_end.strftime("%Y-%m-%d")])
            current = pair_end + timedelta(days=1)
    return date_pairs

# Generiamo le coppie di date giornaliere non sovrapposte
lista_giornaliera = generate_daily_date_pairs(lista_date)

print(f"✅ Generatedate: {len(lista_giornaliera)} coppie")
print(f"Prime 3: {lista_giornaliera[:3]}")


output_dir = 'raw_news_data'
if not os.path.exists(output_dir): os.makedirs(output_dir)

# 4. Daily Sharding Execution Loop - No Overlapping Dates
total_tickers = len(tickers)
total_periods = len(lista_giornaliera)

print(f"\n{'='*80}")
print(f"INIZIO DOWNLOAD - {total_tickers} imprese × {total_periods} periodi")
print(f"{'='*80}\n")

for ticker_idx, ticker in enumerate(tickers, 1):
    file_path = f"{output_dir}/{ticker}_3y.parquet"

    # Checkpoint logic: Skip if shard already exists
    if os.path.exists(file_path):
        print(f"⏭️  [{ticker_idx}/{total_tickers}] {ticker} - dati esistenti, salto.")
        continue

    ticker_news = []
    print(f"\n📡 IMPRESA [{ticker_idx}/{total_tickers}]: {ticker}")
    print(f"{'─'*80}")

    for period_idx, (start_date, end_date) in enumerate(lista_giornaliera, 1):
        # Define daily temporal boundaries (no overlapping)
        params = {
            "symbols": ticker,
            "start": start_date,
            "end": end_date,
            "limit": 1000  # No limit - fetch all available articles
        }

        try:
            # Applica rate limiting prima di fare la richiesta
            limiter.wait_if_needed()
            
            # Aggiunta timeout di 15 secondi per evitare blocchi
            res = requests.get(NEWS_URL, headers=HEADERS, params=params, timeout=15)

            if res.status_code == 200:
                articles = res.json().get('news', [])
                articles_count = len(articles)
                
                status = "✓" if articles_count > 0 else "○"
                print(f"  {status} Periodo [{period_idx}/{total_periods}] ({start_date}→{end_date}): {articles_count} articoli", flush=True)
                
                for art in articles:
                    ticker_news.append({
                        "Ticker": ticker,
                        "Date": art.get('created_at'),
                        "Headline": art.get('headline')
                    })
            elif res.status_code == 429:  # Rate Limit Handling
                print("⚠️  Rate limit raggiunto. Attesa 60 secondi...", flush=True)
                time.sleep(60)
                continue
        except requests.exceptions.Timeout:
            print(f"  ⏱️  Periodo [{period_idx}/{total_periods}] ({start_date}→{end_date}): Timeout - riprova", flush=True)
            time.sleep(2)
        except requests.exceptions.ConnectionError as e:
            print(f"  ❌ Periodo [{period_idx}/{total_periods}] ({start_date}→{end_date}): Errore connessione - {str(e)}", flush=True)
            time.sleep(2)
        except Exception as e:
            print(f"  ❌ Periodo [{period_idx}/{total_periods}] ({start_date}→{end_date}): Errore - {str(e)}", flush=True)
            time.sleep(1)

    # Persistence: Save ticker data to Parquet for I/O efficiency
    if ticker_news:
        df_temp = pd.DataFrame(ticker_news).drop_duplicates(subset=['Headline'])
        df_temp.to_parquet(file_path, index=False)
        print(f"\n   💾 {ticker}: {len(df_temp)} articoli unici salvati.")
    else:
        print(f"\n   ⚠️  {ticker}: Nessun articolo trovato.")

print(f"\n{'='*80}")
print(f"🎉 DOWNLOAD COMPLETATO! Dati archiviati in 'raw_news_data'")
print(f"{'='*80}")

import shutil
from google.colab import files

# 1. Define the directory to be zipped and the output filename
# This matches the folder name 'raw_news_data' from your previous script
dir_to_zip = 'raw_news_data'
output_filename = 'raw_news_data_backup.zip'

# 2. Execute the compression (shutil handles the zipping logic)
print(f"📦 Compressing {dir_to_zip}... Please wait.")
shutil.make_archive(output_filename.replace('.zip', ''), 'zip', dir_to_zip)

# 3. Trigger the browser download
print(f"🚀 Starting download: {output_filename}")
files.download(output_filename)