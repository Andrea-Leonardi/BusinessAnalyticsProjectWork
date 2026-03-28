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
import config as cfg 
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