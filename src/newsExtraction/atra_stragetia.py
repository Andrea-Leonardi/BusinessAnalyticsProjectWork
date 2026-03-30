#%%
import pandas as pd
import sys 
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 
data = pd.read_csv(cfg.ENT)
name_enterprise = data["Ticker"]

# Convertiamo la colonna in una lista di Python, rimuovendo eventuali duplicati e valori nulli

tickers_list = name_enterprise.dropna().unique().tolist()


#creaimo una lista, all'interno delle quali saranno presenti altre liste con due date, la prima è la data di inizio, e la seconda è la data di fine, in questo modo possiamo fare 13 diverse operazioni di raccolta, una per ogni trimestre, e ottenere tutti gli articoli disponibili in quel range di date, senza perdere nessun dato importante.
from datetime import datetime, timedelta

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
    ["2026-01-01", "2026-03-31"], ["2026-04-01", "2026-06-30"],
    ["2026-07-01", "2026-09-30"], ["2026-10-01", "2026-12-31"]
]

def generate_daily_dates(trimesters):
    daily_dates = []
    for start_str, end_str in trimesters:
        start = datetime.strptime(start_str, "%Y-%m-%d")
        end = datetime.strptime(end_str, "%Y-%m-%d")
        current = start
        while current <= end:
            daily_dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
    return daily_dates

# Generiamo la lista giornaliera
lista_giornaliera = generate_daily_dates(lista_date)
# lista_giornaliera = lista_giornaliera[0:2]
# da eliminare dopo la prova, serve solo per vedere se il codice funziona 


# Esempio: stampiamo le prime 10 date
print(lista_giornaliera[:10])



name_enterprise.to_csv("C:\\Users\\emili\\Downloads\\name_enterprise.csv", index=False)


#%%
import requests
import time
import json

# --- LE TUE VARIABILI INIZIALI ---
FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
tickers_list = ["AAPL", "MSFT"] # I ticker che vuoi filtrare
LIMIT_PER_REQUEST = 10000 # Quanti articoli scaricare per pagina

# Lista in cui salveremo solo gli articoli delle imprese che ci interessano
articoli_salvati = []
totale_articoli_scaricati = 0

print("Inizio il processo di estrazione e filtraggio...")
print("-" * 50)

# 1. Ciclo attraverso ogni giorno della tua lista
for current_date in lista_giornaliera:
    current_page = 0
    articoli_trovati_oggi = 0
    
    # Estraiamo mese e anno per il tuo indicatore
    anno, mese, giorno = current_date.split("-")
    
    while True:
        # Chiamata API generica per TUTTE le imprese in quella singola giornata
        url = (
            f"https://financialmodelingprep.com/stable/fmp-articles"
            f"?from={current_date}&to={current_date}"
            f"&page={current_page}&limit={LIMIT_PER_REQUEST}"
            f"&apikey={FMP_API_KEY}"
        )
        
        response = requests.get(url)
        
        # Gestione errori API
        if response.status_code != 200:
            print(f"Errore {response.status_code} nella data {current_date}.")
            break
            
        data = response.json()
        
        # Se non ci sono più articoli per questa pagina/data, usciamo dal ciclo while
        if not data or len(data) == 0:
            break
            
        # 3. Filtraggio Locale
        for articolo in data:
            # Recuperiamo la stringa dei tickers (es: "NASDAQ:AAPL", o a volte più di uno separati da virgola)
            stringa_tickers_api = articolo.get("tickers", "")
            
            # Controlliamo se uno dei nostri ticker di interesse è in quella stringa
            # Usiamo f":{ticker}" per cercare esattamente ":AAPL" ed evitare falsi positivi 
            # (es. se cercassimo solo "A" troveremmo di tutto)
            for mio_ticker in tickers_list:
                if f":{mio_ticker}" in stringa_tickers_api:
                    articoli_salvati.append(articolo)
                    articoli_trovati_oggi += 1
                    totale_articoli_scaricati += 1
                    break # Se lo trova, inutile controllare gli altri ticker per lo stesso articolo
        
        # Passiamo alla pagina successiva per la stessa giornata
        current_page += 1
        
        # Piccola pausa per non sovraccaricare l'API (evita ban temporanei)
        time.sleep(0.2) 

    # 2. L'Indicatore a schermo
    # Visto che cerchiamo più imprese contemporaneamente per data, ti mostro il riepilogo giornaliero
    print(f"[INFO] Data: {current_date} (Mese: {mese}, Anno: {anno}) | "
          f"Articoli salvati oggi: {articoli_trovati_oggi} | "
          f"TOTALE COMPLESSIVO: {totale_articoli_scaricati}")

print("-" * 50)
print(f"Processo completato! Hai scaricato un totale di {totale_articoli_scaricati} articoli utili.")