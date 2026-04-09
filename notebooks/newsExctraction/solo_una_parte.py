#%%
import pandas as pd
import sys 
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 
data = pd.read_csv(cfg.ENT)
name_enterprise = data["Ticker"]

# Convertiamo la colonna in una lista di Python, rimuovendo eventuali duplicati e valori nulli

tickers_list = name_enterprise.dropna().unique().tolist()[1:2]


#creaimo una lista, all'interno delle quali saranno presenti altre liste con due date, la prima è la data di inizio, e la seconda è la data di fine, in questo modo possiamo fare 13 diverse operazioni di raccolta, una per ogni trimestre, e ottenere tutti gli articoli disponibili in quel range di date, senza perdere nessun dato importante.
list_dates_monthly = [
    # 2021
    ["2021-01-01", "2021-01-31"],  # gennaio 2021 (31 giorni)
    ["2021-02-01", "2021-02-28"],  # febbraio 2021 (28 giorni - anno non bisestile)
    ["2021-03-01", "2021-03-31"],  # marzo 2021 (31 giorni)
    ["2021-04-01", "2021-04-30"],  # aprile 2021 (30 giorni)
    ["2021-05-01", "2021-05-31"],  # maggio 2021 (31 giorni)
    ["2021-06-01", "2021-06-30"],  # giugno 2021 (30 giorni)
    ["2021-07-01", "2021-07-31"],  # luglio 2021 (31 giorni)
    ["2021-08-01", "2021-08-31"],  # agosto 2021 (31 giorni)
    ["2021-09-01", "2021-09-30"],  # settembre 2021 (30 giorni)
    ["2021-10-01", "2021-10-31"],  # ottobre 2021 (31 giorni)
    ["2021-11-01", "2021-11-30"],  # novembre 2021 (30 giorni)
    ["2021-12-01", "2021-12-31"],  # dicembre 2021 (31 giorni)
    
    # 2022
    ["2022-01-01", "2022-01-31"],  # gennaio 2022 (31 giorni)
    ["2022-02-01", "2022-02-28"],  # febbraio 2022 (28 giorni - anno non bisestile)
    ["2022-03-01", "2022-03-31"],  # marzo 2022 (31 giorni)
    ["2022-04-01", "2022-04-30"],  # aprile 2022 (30 giorni)
    ["2022-05-01", "2022-05-31"],  # maggio 2022 (31 giorni)
    ["2022-06-01", "2022-06-30"],  # giugno 2022 (30 giorni)
    ["2022-07-01", "2022-07-31"],  # luglio 2022 (31 giorni)
    ["2022-08-01", "2022-08-31"],  # agosto 2022 (31 giorni)
    ["2022-09-01", "2022-09-30"],  # settembre 2022 (30 giorni)
    ["2022-10-01", "2022-10-31"],  # ottobre 2022 (31 giorni)
    ["2022-11-01", "2022-11-30"],  # novembre 2022 (30 giorni)
    ["2022-12-01", "2022-12-31"],  # dicembre 2022 (31 giorni)
    
    # 2023
    ["2023-01-01", "2023-01-31"],  # gennaio 2023 (31 giorni)
    ["2023-02-01", "2023-02-28"],  # febbraio 2023 (28 giorni - anno non bisestile)
    ["2023-03-01", "2023-03-31"],  # marzo 2023 (31 giorni)
    ["2023-04-01", "2023-04-30"],  # aprile 2023 (30 giorni)
    ["2023-05-01", "2023-05-31"],  # maggio 2023 (31 giorni)
    ["2023-06-01", "2023-06-30"],  # giugno 2023 (30 giorni)
    ["2023-07-01", "2023-07-31"],  # luglio 2023 (31 giorni)
    ["2023-08-01", "2023-08-31"],  # agosto 2023 (31 giorni)
    ["2023-09-01", "2023-09-30"],  # settembre 2023 (30 giorni)
    ["2023-10-01", "2023-10-31"],  # ottobre 2023 (31 giorni)
    ["2023-11-01", "2023-11-30"],  # novembre 2023 (30 giorni)
    ["2023-12-01", "2023-12-31"],  # dicembre 2023 (31 giorni)
    
    # 2024 (anno bisestile)
    ["2024-01-01", "2024-01-31"],  # gennaio 2024 (31 giorni)
    ["2024-02-01", "2024-02-29"],  # febbraio 2024 (29 giorni - ANNO BISESTILE)
    ["2024-03-01", "2024-03-31"],  # marzo 2024 (31 giorni)
    ["2024-04-01", "2024-04-30"],  # aprile 2024 (30 giorni)
    ["2024-05-01", "2024-05-31"],  # maggio 2024 (31 giorni)
    ["2024-06-01", "2024-06-30"],  # giugno 2024 (30 giorni)
    ["2024-07-01", "2024-07-31"],  # luglio 2024 (31 giorni)
    ["2024-08-01", "2024-08-31"],  # agosto 2024 (31 giorni)
    ["2024-09-01", "2024-09-30"],  # settembre 2024 (30 giorni)
    ["2024-10-01", "2024-10-31"],  # ottobre 2024 (31 giorni)
    ["2024-11-01", "2024-11-30"],  # novembre 2024 (30 giorni)
    ["2024-12-01", "2024-12-31"],  # dicembre 2024 (31 giorni)
    
    # 2025
    ["2025-01-01", "2025-01-31"],  # gennaio 2025 (31 giorni)
    ["2025-02-01", "2025-02-28"],  # febbraio 2025 (28 giorni - anno non bisestile)
    ["2025-03-01", "2025-03-31"],  # marzo 2025 (31 giorni)
    ["2025-04-01", "2025-04-30"],  # aprile 2025 (30 giorni)
    ["2025-05-01", "2025-05-31"],  # maggio 2025 (31 giorni)
    ["2025-06-01", "2025-06-30"],  # giugno 2025 (30 giorni)
    ["2025-07-01", "2025-07-31"],  # luglio 2025 (31 giorni)
    ["2025-08-01", "2025-08-31"],  # agosto 2025 (31 giorni)
    ["2025-09-01", "2025-09-30"],  # settembre 2025 (30 giorni)
    ["2025-10-01", "2025-10-31"],  # ottobre 2025 (31 giorni)
    ["2025-11-01", "2025-11-30"],  # novembre 2025 (30 giorni)
    ["2025-12-01", "2025-12-31"],  # dicembre 2025 (31 giorni)
    
    # 2026 (fino a marzo)
    ["2026-01-01", "2026-01-31"],  # gennaio 2026 (31 giorni)
    ["2026-02-01", "2026-02-28"],  # febbraio 2026 (28 giorni - anno non bisestile)
    ["2026-03-01", "2026-03-31"],  # marzo 2026 (31 giorni)
]
# %%

#%%
import pandas as pd
import requests
import sys 
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 

# Carica i dati
data = pd.read_csv(cfg.ENT)
tickers_list = data["Ticker"].dropna().unique().tolist()[1:2] 

# Crea un dizionario ticker -> company name
ticker_to_company = dict(zip(data["Ticker"], data["companyName"]))

# Intervalli temporali
list_dates = [
    ["2021-01-01", "2021-03-31"],   # 1° trimestre 2021
    ["2021-04-01", "2021-06-30"],   # 2° trimestre 2021
    ["2021-07-01", "2021-09-30"],   # 3° trimestre 2021
    ["2021-10-01", "2021-12-31"],   # 4° trimestre 2021
    
    ["2022-01-01", "2022-03-31"],   # 1° trimestre 2022
    ["2022-04-01", "2022-06-30"],   # 2° trimestre 2022
    ["2022-07-01", "2022-09-30"],   # 3° trimestre 2022
    ["2022-10-01", "2022-12-31"],   # 4° trimestre 2022
    
    ["2023-01-01", "2023-03-31"],   # 1° trimestre 2023
    ["2023-04-01", "2023-06-30"],   # 2° trimestre 2023
    ["2023-07-01", "2023-09-30"],   # 3° trimestre 2023
    ["2023-10-01", "2023-12-31"],   # 4° trimestre 2023
    
    ["2024-01-01", "2024-03-31"],   # 1° trimestre 2024
    ["2024-04-01", "2024-06-30"],   # 2° trimestre 2024
    ["2024-07-01", "2024-09-30"],   # 3° trimestre 2024
    ["2024-10-01", "2024-12-31"],   # 4° trimestre 2024
    
    ["2025-01-01", "2025-03-31"],   # 1° trimestre 2025
    ["2025-04-01", "2025-06-30"],   # 2° trimestre 2025
    ["2025-07-01", "2025-09-30"],   # 3° trimestre 2025
    ["2025-10-01", "2025-12-31"],   # 4° trimestre 2025
    
    ["2026-01-01", "2026-03-31"],   # 1° trimestre 2026
]

FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
all_news = []

# Cicla gli intervalli di date
total_intervals = len(list_dates)
for interval_num, (start_date, end_date) in enumerate(list_dates, 1):
    print(f"\n{'='*80}")
    print(f"Intervallo {interval_num}/{total_intervals}: {start_date} - {end_date}")
    print(f"{'='*80}")
    
    # Cicla i ticker
    total_tickers = len(tickers_list)
    for ticker_num, ticker in enumerate(tickers_list, 1):
        company_name = ticker_to_company.get(ticker, "N/A")
        try:
            # Usa il nuovo endpoint stabile di FMP
            url = f"https://financialmodelingprep.com/stable/fmp-articles?page=0&limit=1000&apikey={FMP_API_KEY}"
            response = requests.get(url, timeout=10)
            news_data = response.json()
            
            # Verifica la struttura della risposta
            if isinstance(news_data, dict) and 'Error Message' in news_data:
                print(f"  [{ticker_num:3d}/{total_tickers}] {ticker:6s} | {company_name:30s} | ERRORE API: {news_data['Error Message'][:80]}")
                continue
            
            if not isinstance(news_data, list):
                print(f"  [{ticker_num:3d}/{total_tickers}] {ticker:6s} | {company_name:30s} | ERRORE: Tipo={type(news_data).__name__}")
                continue
            
            # Filtra gli articoli per ticker e data intervallo
            ticker_news = []
            for article in news_data:
                # Verifica se il ticker è nel titolo o nel testo dell'articolo
                title = article.get('title', '').upper()
                text = article.get('text', '').upper()
                ticker_upper = ticker.upper()
                
                if ticker_upper in title or ticker_upper in text:
                    # Verifica che l'articolo sia entro il range di date
                    pub_date = article.get('publishedDate', '')
                    if start_date <= pub_date <= end_date:
                        article['ticker'] = ticker
                        article['company'] = company_name
                        ticker_news.append(article)
                        all_news.append(article)
            
            progress_pct = (ticker_num / total_tickers) * 100
            print(f"  [{ticker_num:3d}/{total_tickers}] {ticker:6s} | {company_name:30s} | OK {len(ticker_news):3d} notizie | Totale: {len(all_news):6d} | {progress_pct:5.1f}%")
            
        except Exception as e:
            progress_pct = (ticker_num / total_tickers) * 100
            print(f"  [{ticker_num:3d}/{total_tickers}] {ticker:6s} | {company_name:30s} | ERRORE: {e} | Totale: {len(all_news):6d} | {progress_pct:5.1f}%")

# Salva tutto in un file
df_news = pd.DataFrame(all_news)
df_news.to_csv("news_estratte.csv", index=False)
print(f"\n{'='*80}")
print(f"Completato! Totale notizie estratte: {len(df_news)}")
print(f"{'='*80}")



