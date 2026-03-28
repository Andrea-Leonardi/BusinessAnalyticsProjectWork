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