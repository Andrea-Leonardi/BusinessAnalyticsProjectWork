#%%
import json
from urllib.request import urlopen

import certifi


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
FMP_ARTICLES_URL = (
    "https://financialmodelingprep.com/stable/fmp-articles"
    f"?page=0&limit=20&apikey={FMP_API_KEY}"
)


def get_jsonparsed_data(url: str):
    """Download one JSON payload and convert it to Python objects."""
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)


# ---------------------------------------------------------------------------
# Download And Print News
# ---------------------------------------------------------------------------

articles = get_jsonparsed_data(FMP_ARTICLES_URL)

if not isinstance(articles, list) or not articles:
    print("No news articles were returned by the API.")
else:
    print(f"Downloaded {len(articles)} news articles.\n")

    for index, article in enumerate(articles, start=1):
        title = article.get("title", "No title")
        article_link = article.get("link") or article.get("url") or "No URL"

        print(f"{index}. {title}")
        print(f"   {article_link}")
        print()

# %%






#%%

import json
import ssl
from urllib.request import urlopen
import certifi

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
FMP_ARTICLES_URL = (
    "https://financialmodelingprep.com/stable/fmp-articles"
    f"?page=0&limit=20&apikey={FMP_API_KEY}"
)
def get_jsonparsed_data(url: str):
    """Download one JSON payload and convert it to Python objects."""
    
    # crea contesto SSL con certificati validi
    context = ssl.create_default_context(cafile=certifi.where())

    response = urlopen(url, context=context)
    data = response.read().decode("utf-8")
    return json.loads(data)
# ---------------------------------------------------------------------------
# Download And Print News
# ---------------------------------------------------------------------------


articles = get_jsonparsed_data(FMP_ARTICLES_URL)

print("DEBUG tipo:", type(articles))
print("DEBUG contenuto:", str(articles)[:200], "\n")

# gestione struttura API
if isinstance(articles, dict) and "content" in articles:
    articles = articles["content"]

if not isinstance(articles, list) or not articles:
    print("No news articles were returned by the API.")
    print("Risposta:", articles)
else:
    print(f"Downloaded {len(articles)} news articles.\n")

    for index, article in enumerate(articles, start=1):
        title = article.get("title", "No title")
        article_link = article.get("link") or article.get("url") or "No URL"

        print(f"{index}. {title}")
        print(f"   {article_link}")
        print()

# %%


#%%
import json
import ssl
import time  # <--- Necessario per le pause
from urllib.request import urlopen
from urllib.error import HTTPError  # <--- Per gestire gli errori del server
import certifi

FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
FMP_ARTICLES_URL = (
    "https://financialmodelingprep.com/stable/fmp-articles"
    f"?page=0&limit=20&apikey={FMP_API_KEY}"
)

def get_jsonparsed_data(url: str):
    context = ssl.create_default_context(cafile=certifi.where())
    
    try:
        response = urlopen(url, context=context)
        data = response.read().decode("utf-8")
        return json.loads(data)
    
    except HTTPError as e:
        if e.code == 429:
            print("⚠️ Limite raggiunto! Aspetto 5 secondi e riprovo...")
            time.sleep(5)
            return get_jsonparsed_data(url) # Riprova la chiamata
        else:
            print(f"❌ Errore HTTP: {e.code}")
            return None

# --- ESEMPIO DI CICLO PER PIÙ PAGINE ---
all_articles = []
for page in range(0, 5):  # Ipotizziamo di voler scaricare le prime 5 pagine
    print(f"Scaricamento pagina {page}...")
    
    url = (
        f"https://financialmodelingprep.com/stable/fmp-articles"
        f"?page={page}&from=2026-01-01&limit=100&apikey={FMP_API_KEY}"
    )
    
    data = get_jsonparsed_data(url)
    
    if data:
        # Gestione struttura (se è un dizionario con "content" o lista diretta)
        articles_list = data["content"] if isinstance(data, dict) and "content" in data else data
        all_articles.extend(articles_list)
    
    # --- IL LIMITATORE ---
    # Aspetta 0.5 secondi prima della prossima richiesta (max 120 req/min)
    time.sleep(0.5) 

print(f"\n✅ Totale articoli recuperati: {len(all_articles)}")

# %%












"""
questo è il codice che funziona e ci permette di estrarre tutti gli articoli pubblicati tra il 01-01-2021 e il 01-03-2026, senza limiti di pagine, e gestendo correttamente i limiti di velocità dell'API, in modo da non venire bloccati.
Mentre le versioni precedenti sono solo delle prove per capire come funziona l'API e come gestire i dati, questa versione è quella definitiva che ci permette di scaricare TUTTI gli articoli disponibili in quel range di date, e di salvarli in una lista chiamata "all_articles", che poi potremo trasformare in un DataFrame per le analisi successive.
probabilmente ci sono delle limitazioni al numero di pagine a cui possiamo accedere per estrarre informazioni, per questo dividiamo in due le richeste di articoli, ossia con la prima richiesta 
otteniamo gli articoli dal 2021-01-01 al 2023-12-31, e con la seconda richiesta otteniamo gli articoli dal 2024-01-01 al 2026-03-27, in questo modo evitiamo di superare il limite di pagine e di articoli che possiamo scaricare in un'unica chiamata, e ci assicuriamo di ottenere tutti gli articoli disponibili in quel range di date.
andche dividendo in questo modo si è raggiunto il limite, quindi per evitare problemi faccio 6 diverse operazioni di raccolta, una per ogni anno
quindi ad ogni operazione si raccolgono gli articoli di un anno dal 01-01 fino a 12-01 per ogni anno rispettivo, in questo modo si evita di superare il limite di pagine e di articoli che si possono scaricare in un'unica chiamata, e si riesce a ottenere tutti gli articoli disponibili in quel range di date, senza perdere nessun dato importante.
"""


#%%
import json
import ssl
import time
from urllib.request import urlopen
from urllib.error import HTTPError
import certifi


# --- CONFIGURAZIONE ---
FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
START_DATE = "2026-01-01"
END_DATE = "2026-03-27"
LIMIT_PER_REQUEST = 100 # Massimo consentito per singola chiamata

def get_jsonparsed_data(url: str):
    context = ssl.create_default_context(cafile=certifi.where())
    try:
        response = urlopen(url, context=context)
        return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        if e.code == 429:
            print("⚠️ Limite raggiunto! Aspetto 10 secondi...")
            time.sleep(10)
            return get_jsonparsed_data(url)
        print(f"❌ Errore HTTP {e.code}")
        return None

# --- LOGICA DI SCARICAMENTO ILLIMITATO ---
all_articles_2026 = []
current_page = 0
keep_going = True

print(f"🚀 Inizio download articoli dal {START_DATE} al {END_DATE}...")

while keep_going:
    # Costruiamo la URL per la pagina corrente
    url = (
        f"https://financialmodelingprep.com/stable/fmp-articles"
        f"?from={START_DATE}&to={END_DATE}"
        f"&page={current_page}&limit={LIMIT_PER_REQUEST}"
        f"&apikey={FMP_API_KEY}"
    )

    data = get_jsonparsed_data(url)

    # Gestione della struttura (estrazione della lista di articoli)
    articles_chunk = []
    if isinstance(data, dict) and "content" in data:
        articles_chunk = data["content"]
    elif isinstance(data, list):
        articles_chunk = data

    # Se la lista è vuota, abbiamo finito tutti gli articoli disponibili
    if not articles_chunk:
        print("\n✅ Nessun altro articolo trovato. Download completato!")
        keep_going = False
    else:
        all_articles_2026.extend(articles_chunk)
        print(f"📦 Scaricata pagina {current_page} ({len(all_articles_2026)} articoli totali...)")
        
        # Passiamo alla pagina successiva
        current_page += 1
        
        # --- LIMITATORE DI VELOCITÀ ---
        # 300 richieste/minuto = 1 richiesta ogni 0.2 secondi.
        # Usiamo 0.3 secondi per essere sicuri al 100% di non venire bloccati.
        time.sleep(0.3)

# --- RISULTATO FINALE ---
print(f"\n🏆 Operazione conclusa!")
print(f"In totale sono stati scaricati {len(all_articles_2026)} articoli.")


"""
-------------------------------------------------------------------------------------------------------------
"""

# --- CONFIGURAZIONE ---
FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
START_DATE = "2025-01-01"
END_DATE = "2025-12-31"
LIMIT_PER_REQUEST = 100 # Massimo consentito per singola chiamata

def get_jsonparsed_data(url: str):
    context = ssl.create_default_context(cafile=certifi.where())
    try:
        response = urlopen(url, context=context)
        return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        if e.code == 429:
            print("⚠️ Limite raggiunto! Aspetto 10 secondi...")
            time.sleep(10)
            return get_jsonparsed_data(url)
        print(f"❌ Errore HTTP {e.code}")
        return None

all_articles_2025 = []
current_page = 0
keep_going = True

print(f"🚀 Inizio download articoli dal {START_DATE} al {END_DATE}...")

while keep_going:
    # Costruiamo la URL per la pagina corrente
    url = (
        f"https://financialmodelingprep.com/stable/fmp-articles"
        f"?from={START_DATE}&to={END_DATE}"
        f"&page={current_page}&limit={LIMIT_PER_REQUEST}"
        f"&apikey={FMP_API_KEY}"
    )

    data = get_jsonparsed_data(url)

    # Gestione della struttura (estrazione della lista di articoli)
    articles_chunk = []
    if isinstance(data, dict) and "content" in data:
        articles_chunk = data["content"]
    elif isinstance(data, list):
        articles_chunk = data

    # Se la lista è vuota, abbiamo finito tutti gli articoli disponibili
    if not articles_chunk:
        print("\n✅ Nessun altro articolo trovato. Download completato!")
        keep_going = False
    else:
        all_articles_2025.extend(articles_chunk)
        print(f"📦 Scaricata pagina {current_page} ({len(all_articles_2025)} articoli totali...)")
        
        # Passiamo alla pagina successiva
        current_page += 1
        
        # --- LIMITATORE DI VELOCITÀ ---
        # 300 richieste/minuto = 1 richiesta ogni 0.2 secondi.
        # Usiamo 0.3 secondi per essere sicuri al 100% di non venire bloccati.
        time.sleep(0.3)

# --- RISULTATO FINALE ---
print(f"\n🏆 Operazione conclusa!")
print(f"In totale sono stati scaricati {len(all_articles_2025)} articoli.")


"""
-------------------------------------------------------------------------------------------------------------
"""

# --- CONFIGURAZIONE ---
FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"
LIMIT_PER_REQUEST = 100 # Massimo consentito per singola chiamata

def get_jsonparsed_data(url: str):
    context = ssl.create_default_context(cafile=certifi.where())
    try:
        response = urlopen(url, context=context)
        return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        if e.code == 429:
            print("⚠️ Limite raggiunto! Aspetto 10 secondi...")
            time.sleep(10)
            return get_jsonparsed_data(url)
        print(f"❌ Errore HTTP {e.code}")
        return None

all_articles_2024 = []
current_page = 0
keep_going = True

print(f"🚀 Inizio download articoli dal {START_DATE} al {END_DATE}...")

while keep_going:
    # Costruiamo la URL per la pagina corrente
    url = (
        f"https://financialmodelingprep.com/stable/fmp-articles"
        f"?from={START_DATE}&to={END_DATE}"
        f"&page={current_page}&limit={LIMIT_PER_REQUEST}"
        f"&apikey={FMP_API_KEY}"
    )

    data = get_jsonparsed_data(url)

    # Gestione della struttura (estrazione della lista di articoli)
    articles_chunk = []
    if isinstance(data, dict) and "content" in data:
        articles_chunk = data["content"]
    elif isinstance(data, list):
        articles_chunk = data

    # Se la lista è vuota, abbiamo finito tutti gli articoli disponibili
    if not articles_chunk:
        print("\n✅ Nessun altro articolo trovato. Download completato!")
        keep_going = False
    else:
        all_articles_2024.extend(articles_chunk)
        print(f"📦 Scaricata pagina {current_page} ({len(all_articles_2024)} articoli totali...)")
        
        # Passiamo alla pagina successiva
        current_page += 1
        
        # --- LIMITATORE DI VELOCITÀ ---
        # 300 richieste/minuto = 1 richiesta ogni 0.2 secondi.
        # Usiamo 0.3 secondi per essere sicuri al 100% di non venire bloccati.
        time.sleep(0.3)

# --- RISULTATO FINALE ---
print(f"\n🏆 Operazione conclusa!")
print(f"In totale sono stati scaricati {len(all_articles_2024)} articoli.")


"""
-------------------------------------------------------------------------------------------------------------
"""

# --- CONFIGURAZIONE ---
FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
START_DATE = "2023-01-01"
END_DATE = "2023-12-31"
LIMIT_PER_REQUEST = 100 # Massimo consentito per singola chiamata

def get_jsonparsed_data(url: str):
    context = ssl.create_default_context(cafile=certifi.where())
    try:
        response = urlopen(url, context=context)
        return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        if e.code == 429:
            print("⚠️ Limite raggiunto! Aspetto 10 secondi...")
            time.sleep(10)
            return get_jsonparsed_data(url)
        print(f"❌ Errore HTTP {e.code}")
        return None

all_articles_2023 = []
current_page = 0
keep_going = True

print(f"🚀 Inizio download articoli dal {START_DATE} al {END_DATE}...")

while keep_going:
    # Costruiamo la URL per la pagina corrente
    url = (
        f"https://financialmodelingprep.com/stable/fmp-articles"
        f"?from={START_DATE}&to={END_DATE}"
        f"&page={current_page}&limit={LIMIT_PER_REQUEST}"
        f"&apikey={FMP_API_KEY}"
    )

    data = get_jsonparsed_data(url)

    # Gestione della struttura (estrazione della lista di articoli)
    articles_chunk = []
    if isinstance(data, dict) and "content" in data:
        articles_chunk = data["content"]
    elif isinstance(data, list):
        articles_chunk = data

    # Se la lista è vuota, abbiamo finito tutti gli articoli disponibili
    if not articles_chunk:
        print("\n✅ Nessun altro articolo trovato. Download completato!")
        keep_going = False
    else:
        all_articles_2023.extend(articles_chunk)
        print(f"📦 Scaricata pagina {current_page} ({len(all_articles_2023)} articoli totali...)")
        
        # Passiamo alla pagina successiva
        current_page += 1
        
        # --- LIMITATORE DI VELOCITÀ ---
        # 300 richieste/minuto = 1 richiesta ogni 0.2 secondi.
        # Usiamo 0.3 secondi per essere sicuri al 100% di non venire bloccati.
        time.sleep(0.3)

# --- RISULTATO FINALE ---
print(f"\n🏆 Operazione conclusa!")
print(f"In totale sono stati scaricati {len(all_articles_2023)} articoli.")


"""
-------------------------------------------------------------------------------------------------------------
"""

# --- CONFIGURAZIONE ---
FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
START_DATE = "2022-01-01" 
END_DATE = "2022-12-31"
LIMIT_PER_REQUEST = 100 # Massimo consentito per singola chiamata

def get_jsonparsed_data(url: str):
    context = ssl.create_default_context(cafile=certifi.where())
    try:
        response = urlopen(url, context=context)
        return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        if e.code == 429:
            print("⚠️ Limite raggiunto! Aspetto 10 secondi...")
            time.sleep(10)
            return get_jsonparsed_data(url)
        print(f"❌ Errore HTTP {e.code}")
        return None
all_articles_2022 = []
current_page = 0
keep_going = True

print(f"🚀 Inizio download articoli dal {START_DATE} al {END_DATE}...")

while keep_going:
    # Costruiamo la URL per la pagina corrente
    url = (
        f"https://financialmodelingprep.com/stable/fmp-articles"
        f"?from={START_DATE}&to={END_DATE}"
        f"&page={current_page}&limit={LIMIT_PER_REQUEST}"
        f"&apikey={FMP_API_KEY}"
    )

    data = get_jsonparsed_data(url)

    # Gestione della struttura (estrazione della lista di articoli)
    articles_chunk = []
    if isinstance(data, dict) and "content" in data:
        articles_chunk = data["content"]
    elif isinstance(data, list):
        articles_chunk = data

    # Se la lista è vuota, abbiamo finito tutti gli articoli disponibili
    if not articles_chunk:
        print("\n✅ Nessun altro articolo trovato. Download completato!")
        keep_going = False
    else:
        all_articles_2022.extend(articles_chunk)
        print(f"📦 Scaricata pagina {current_page} ({len(all_articles_2022)} articoli totali...)")
        
        # Passiamo alla pagina successiva
        current_page += 1
        
        # --- LIMITATORE DI VELOCITÀ ---
        # 300 richieste/minuto = 1 richiesta ogni 0.2 secondi.
        # Usiamo 0.3 secondi per essere sicuri al 100% di non venire bloccati.
        time.sleep(0.3)

# --- RISULTATO FINALE ---
print(f"\n🏆 Operazione conclusa!")
print(f"In totale sono stati scaricati {len(all_articles_2022)} articoli.")


"""
-------------------------------------------------------------------------------------------------------------
"""

# --- CONFIGURAZIONE ---
FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
START_DATE = "2021-01-01"
END_DATE = "2021-12-31"
LIMIT_PER_REQUEST = 100 # Massimo consentito per singola chiamata

def get_jsonparsed_data(url: str):
    context = ssl.create_default_context(cafile=certifi.where())
    try:
        response = urlopen(url, context=context)
        return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        if e.code == 429:
            print("⚠️ Limite raggiunto! Aspetto 10 secondi...")
            time.sleep(10)
            return get_jsonparsed_data(url)
        print(f"❌ Errore HTTP {e.code}")
        return None
all_articles_2021 = []
current_page = 0
keep_going = True

print(f"🚀 Inizio download articoli dal {START_DATE} al {END_DATE}...")

while keep_going:
    # Costruiamo la URL per la pagina corrente
    url = (
        f"https://financialmodelingprep.com/stable/fmp-articles"
        f"?from={START_DATE}&to={END_DATE}"
        f"&page={current_page}&limit={LIMIT_PER_REQUEST}"
        f"&apikey={FMP_API_KEY}"
    )

    data = get_jsonparsed_data(url)

    # Gestione della struttura (estrazione della lista di articoli)
    articles_chunk = []
    if isinstance(data, dict) and "content" in data:
        articles_chunk = data["content"]
    elif isinstance(data, list):
        articles_chunk = data

    # Se la lista è vuota, abbiamo finito tutti gli articoli disponibili
    if not articles_chunk:
        print("\n✅ Nessun altro articolo trovato. Download completato!")
        keep_going = False
    else:
        all_articles_2021.extend(articles_chunk)
        print(f"📦 Scaricata pagina {current_page} ({len(all_articles_2021)} articoli totali...)")
        
        # Passiamo alla pagina successiva
        current_page += 1
        
        # --- LIMITATORE DI VELOCITÀ ---
        # 300 richieste/minuto = 1 richiesta ogni 0.2 secondi.
        # Usiamo 0.3 secondi per essere sicuri al 100% di non venire bloccati.
        time.sleep(0.3)

# --- RISULTATO FINALE ---
print(f"\n🏆 Operazione conclusa!")
print(f"In totale sono stati scaricati {len(all_articles_2021)} articoli.")



"""
-------------------------------------------------------------------------------------------------------------
"""



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
# %%

#%%
import json
import ssl
import time
from urllib.request import urlopen
from urllib.error import HTTPError
import certifi
# EMILIANO


# --- CONFIGURAZIONE ---
FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7" # Inserisci la chiave rigenerata!
START_DATE = "2026-01-01"
END_DATE = "2026-03-27"
LIMIT_PER_REQUEST = 300

def get_jsonparsed_data(url: str):
    context = ssl.create_default_context(cafile=certifi.where())
    try:
        response = urlopen(url, context=context)
        return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        if e.code == 429:
            print("⚠️ Limite raggiunto! Aspetto 10 secondi...")
            time.sleep(10)
            return get_jsonparsed_data(url)
        print(f"❌ Errore HTTP {e.code} sull'URL: {url}")
        return None

# --- LOGICA DI SCARICAMENTO MIRATO ---
all_articles_2026 = []

print(f"🚀 Inizio download articoli per {len(tickers_list)} aziende dal {START_DATE} al {END_DATE}...")

# Iteriamo su ogni singolo ticker della tua lista
for ticker in tickers_list:
    print(f"\n🔍 Inizio ricerca per il ticker: {ticker}")
    current_page = 0
    keep_going = True
    
    while keep_going:
        # Aggiungiamo il parametro 'tickers={ticker}' all'URL
        url = (
                f"https://financialmodelingprep.com/api/v3/stock_news"
                f"?tickers=AAPL"  # Sostituisci con {ticker} per iterare su tutti i ticker
                f"&page={current_page}"
                f"&limit={LIMIT_PER_REQUEST}"
                f"&from={START_DATE}"
                f"&to={END_DATE}"
                f"&apikey={FMP_API_KEY}"
            )

        data = get_jsonparsed_data(url)

        # Gestione della struttura
        articles_chunk = []
        if isinstance(data, dict) and "content" in data:
            articles_chunk = data["content"]
        elif isinstance(data, list):
            articles_chunk = data

        # Se la lista è vuota, abbiamo finito gli articoli per QUESTO ticker
        if not articles_chunk:
            print(f"✅ Nessun altro articolo per {ticker}. Passiamo al prossimo.")
            keep_going = False
        else:
            all_articles_2026.extend(articles_chunk)
            print(f"📦 {ticker} - Scaricata pagina {current_page} ({len(articles_chunk)} articoli trovati)")
            
            current_page += 1
            
            # Limitatore di velocità (0.3 secondi)
            time.sleep(0.3)

# --- RISULTATO FINALE ---
print(f"\n🏆 Operazione conclusa!")
print(f"In totale sono stati scaricati {len(all_articles_2026)} articoli per le tue aziende.")













#%%
import requests
import time

# 1. I tuoi dati di partenza
tickers_list = ["AAPL", "MSFT"]
list_dates = [
    ["2025-01-01", "2025-03-20"] # Prova con date recenti
]

FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
LIMIT_PER_REQUEST = 50

# Variabili per immagazzinare i dati e fare il conteggio
all_articles = []
total_articles_downloaded = 0

# 2. Ciclo sui Ticker e sulle Date
for ticker in tickers_list:
    for date_range in list_dates:
        start_date = date_range[0]
        end_date = date_range[1]
        
        current_page = 0 # FMP solitamente parte dalla pagina 0
        
        while True:
            # 3. IL FIX DELL'ENDPOINT: usiamo /api/v3/stock_news
            url = (
                f"https://financialmodelingprep.com/api/v3/stock_news"
                f"?tickers={ticker}"
                f"&page={current_page}"
                f"&limit={LIMIT_PER_REQUEST}"
                f"&from=2026-01-01"
                f"&to=2026-03-27"
                f"&apikey={FMP_API_KEY}"
            )
            
            response = requests.get(url)
            
            # Controllo che la chiamata sia andata a buon fine
            if response.status_code != 200:
                print(f"⚠️ Errore API {response.status_code} per {ticker} nel periodo {start_date} -> {end_date}")
                break
            
            data = response.json()
            
            # Se la pagina è vuota, significa che non ci sono più articoli per questo trimestre. Usciamo dal while.
            if not data:
                break
                
            all_articles.extend(data)
            total_articles_downloaded += len(data)
            
            # 4. L'INDICATORE DI PROGRESSO
            print(f"📌 Impresa: {ticker} | 📅 Periodo: {start_date} al {end_date} | 📑 Articoli totali scaricati: {total_articles_downloaded}")
            
            current_page += 1
            
            # Buona pratica: una piccolissima pausa per non sovraccaricare il server FMP ed evitare blocchi
            time.sleep(0.1) 

print("\n✅ Download completato!")




















































#%%
all_articles = []
all_articles.extend(all_articles_2026)# Uniamo le due liste di articoli
all_articles.extend(all_articles_2025)# Uniamo le due liste di articoli
all_articles.extend(all_articles_2024)# Uniamo le due liste di articoli
all_articles.extend(all_articles_2023)# Uniamo le due liste di articoli
all_articles.extend(all_articles_2022)# Uniamo le due liste di articoli
all_articles.extend(all_articles_2021)# Uniamo le due liste di articoli


#%%

"""
qui dopo aver improtato i dati, rendiamo la lista annidata un dataframe, eliminiamo la colonna image, che non serve
rinominiamo la colonna tickers in enterprise, e puliamo il testo della colonna content, eliminando i tag html e i \n
in fine, dalla colonna enterprise, eliminiamo la parte "STOCK:" e lasciamo solo il ticker, es: "PWR", quindi togliamo il nome 
del mercato in cui è quotata e lasciamo solo il nome dell'azienda, che è quello che ci serve per fare il match con i dati di bilancio e di stock price

"""
#%%

import pandas as pd
from bs4 import BeautifulSoup

# 1. Creiamo il DataFrame dalla tua lista di dizionari
df = pd.DataFrame(all_articles)

# 2. Puliamo il valore in 'tickers' (prendiamo solo quello dopo i :)
# Esempio: "STOCK:PWR" diventa "PWR"
df['tickers'] = df['tickers'].str.split(':').str[1]
df['market'] = df['tickers'].str.split(':').str[0]

# 3. Rinominiamo la colonna 'tickers' in 'enterprise'
df = df.rename(columns={'tickers': 'enterprise'})

# 4. Selezioniamo e ordiniamo le colonne (escludendo 'image')
colonne_finali = ['enterprise', 'date', 'title', 'content', 'link', 'author', 'site']
df = df[colonne_finali]

# Visualizziamo il risultato finale
print(df.head())

# Funzione per pulire ogni singolo testo
def pulisci_testo(testo_sporco):
    if pd.isna(testo_sporco): # Gestisce eventuali valori vuoti
        return ""
    # BeautifulSoup rimuove tutti i tag <p>, <span>, ecc.
    zuppa = BeautifulSoup(testo_sporco, "html.parser")
    testo_pulito = zuppa.get_text(separator=" ") # Mette uno spazio tra i paragrafi
    # .strip() toglie gli spazi bianchi e i \n all'inizio e alla fine
    return testo_pulito.strip()

# Applichiamo la funzione alla colonna 'content'
df['content'] = df['content'].apply(pulisci_testo)

# Ora puoi vedere il testo pulito
print(df.at[0, 'content'])

#%%

#%%
df.sort_values(by=["date", "enterprise"], ascending=[False, True], inplace=True)
df.to_csv(cfg.NEWS_ARTICLES, index=False, encoding='utf-8-sig')
#%%

"""
qui si testano 4 diversi modelli di intelligenza artificiale per l'analisi del sentiment e delle emozioni, e uno per la classificazione zero-shot, che permette di classificare un testo in categorie personalizzate senza bisogno di addestramento specifico
e ogni modello è testato in una diversa sezione del codice, in modo da poter confrontare i risultati e capire quale modello è più adatto per l'analisi delle news finanziarie
"""
#%%

from transformers import BertTokenizer, BertForSequenceClassification
from transformers import pipeline

# 1. Caricamento del modello FinBERT pre-addestrato
model_name = "ProsusAI/finbert"
tokenizer = BertTokenizer.from_pretrained(model_name)
model = BertForSequenceClassification.from_pretrained(model_name)

# 2. Creazione della "pipeline" per l'analisi del sentiment
nlp = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)

# 3. Test su un titolo finanziario
results = nlp(df.at[1, "content"], top_k=None)

print(results)


#%%


#%%

# Modello per 28 emozioni diverse (inclusa la paura, utile in finanza)
nlp_emotion = pipeline("sentiment-analysis", model="SamLowe/roberta-base-go_emotions")

results = nlp_emotion(df.at[1, "content"])
print(results)
# Output tipico: [{'label': 'fear', 'score': 0.92}]


#%%


#%%

from transformers import pipeline

# 1. Carichiamo il modello specifico per le emozioni (GoEmotions)
classifier = pipeline("sentiment-analysis", 
                      model="SamLowe/roberta-base-go_emotions", 
                      top_k=None) # top_k=None serve per avere TUTTE le emozioni

# 2. Analizziamo un testo (es. un commento o un estratto di news)
testo = df.at[1, "content"]
risultati = classifier(testo)

# 3. Stampiamo le prime 5 emozioni rilevate (ordinate per score)
for emozione in risultati[0][:5]:
    print(f"Emozione: {emozione['label']} - Score: {round(emozione['score'], 4)}")


#%%

#%%


from textblob import TextBlob
res = TextBlob(df.at[1, "content"])
print(res.sentiment.subjectivity) 


#%%

#%%

from transformers import pipeline

# Modello: facebook/bart-large-mnli
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

testo = df.at[1, "content"]
labels = ["merger", "bankruptcy", "technological innovation", "legal issue"]

res = classifier(testo, candidate_labels=labels)
print(res)
# Ti darà una percentuale per ogni etichetta che hai scelto tu!

#%%



#%%
import pandas as pd
import requests
import time
from datetime import datetime

# ==============================
# CONFIG
# ==============================
FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
REQUEST_DELAY = 0.2
MAX_PAGES = 200

# ==============================
# LOAD TICKERS
# ==============================
data = pd.read_csv(cfg.ENT)
tickers_list = data["Ticker"].dropna().unique().tolist()

total_tickers = len(tickers_list)
total_intervals = len(list_dates_monthly)

# ==============================
# REQUEST
# ==============================
def get_data(url):
    try:
        res = requests.get(url)
        time.sleep(REQUEST_DELAY)
        
        if res.status_code != 200:
            print(f"\n❌ HTTP {res.status_code}")
            return None
        
        return res.json()
    
    except Exception as e:
        print(f"\n❌ ERRORE: {e}")
        return None

# ==============================
# FETCH PER INTERVALLO
# ==============================
def fetch_interval(ticker, start_date, end_date, ticker_idx, interval_idx, depth=0):
    
    local_articles = []
    page = 0
    
    while True:
        url = (
            f"https://financialmodelingprep.com/stable/fmp-articles"
            f"?tickers={ticker}"
            f"&from={start_date}&to={end_date}"
            f"&page={page}&limit=100"
            f"&apikey={FMP_API_KEY}"
        )
        
        data = get_data(url)
        
        if data is None:
            break
        
        # parsing
        if isinstance(data, dict) and "content" in data:
            articles = data["content"]
        elif isinstance(data, list):
            articles = data
        else:
            articles = []
        
        if not articles:
            break
        
        local_articles.extend(articles)
        
        # ==============================
        # 📊 PROGRESS
        # ==============================
        print(
            f"\r📊 [{interval_idx}/{total_intervals}] "
            f"[{ticker_idx}/{total_tickers}] "
            f"{ticker:6s} | "
            f"{start_date} → {end_date} | "
            f"page {page:3d} | "
            f"+{len(articles):3d} | "
            f"Tot intervallo: {len(local_articles):5d}",
            end=""
        )
        
        page += 1
        
        # ==============================
        # 🚨 LIMITE PAGINE
        # ==============================
        if page >= MAX_PAGES:
            print(f"\n⚠️ SPLIT → {ticker} | {start_date} → {end_date}")
            break
    
    # ==============================
    # 🔁 SPLIT SE NECESSARIO
    # ==============================
    if page >= MAX_PAGES and depth < 5:
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        mid_dt = start_dt + (end_dt - start_dt) / 2
        
        left = fetch_interval(
            ticker,
            start_dt.strftime("%Y-%m-%d"),
            mid_dt.strftime("%Y-%m-%d"),
            ticker_idx,
            interval_idx,
            depth + 1
        )
        
        right = fetch_interval(
            ticker,
            mid_dt.strftime("%Y-%m-%d"),
            end_dt.strftime("%Y-%m-%d"),
            ticker_idx,
            interval_idx,
            depth + 1
        )
        
        return left + right
    
    return local_articles

# ==============================
# MAIN
# ==============================
all_articles = []
global_start = time.time()

for interval_idx, (start_date, end_date) in enumerate(list_dates_monthly, 1):
    
    print(f"\n\n📅 INTERVALLO {interval_idx}/{total_intervals} → {start_date} - {end_date}")
    
    for ticker_idx, ticker in enumerate(tickers_list, 1):
        
        articles = fetch_interval(
            ticker,
            start_date,
            end_date,
            ticker_idx,
            interval_idx
        )
        
        print(f"\n✅ {ticker} | {start_date} → {end_date} → {len(articles)} articoli")
        
        all_articles.extend(articles)

# ==============================
# CLEAN + SAVE
# ==============================
df = pd.DataFrame(all_articles)

print("\n🧹 Rimozione duplicati...")
df = df.drop_duplicates(subset=["title", "publishedDate"])

df.to_csv("news_complete.csv", index=False)

# ==============================
# STATS FINALI
# ==============================
elapsed = time.time() - global_start

print("\n🏆 COMPLETATO")
print(f"📊 Totale articoli: {len(df)}")
print(f"⏱️ Tempo totale: {elapsed/60:.2f} minuti")




#%%
from newspaper import Article
url = "https://www.benzinga.com/trading-ideas/long-ideas/21/01/19015660/will-alibaba-or-pinduoduo-stock-grow-more-by-2022"
article = Article(url)
article.download()
article.parse()

print(f"✅ LIBRERIA CARICATA CON SUCCESSO")
print(f"TITOLO: {article.title}")
print("-" * 30)
print(f"TESTO ESTRATTO:\n{article.text}")
# %%
prova = pd.re


def nome_funzione x: 
    if x["colonna1"].is.null():
        article = Articole(x["url"])
        x["colonna2"] = article.text
    retunr x

# %%
import pandas as pd
from newspaper import Article

df = pd.read_csv('news_dataset_full_text_0_30.csv')
df = df.iloc[0:10, :]

def imputazione_articoli_mancanti(row):
    if pd.isna(row['content']):
        try:
            article = Article(row['link'])
            article.download()
            article.parse()
            return article.text
        except Exception as e:
            print(f"Errore nell'estrazione dell'articolo da {row['link']}: {e}")
            return None
    else:
        return row['content']
