#%%
"""
qui prepariamo i dataset e le relative tabelle che poi usiamo per andare a rimepire il database relazionale.
"""
import pandas as pd
import sys 
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 
from datetime import date, timedelta

aziende_temp = pd.read_csv(cfg.ENT).sort_values(by='Ticker')
articoli_temp = pd.read_csv(cfg.NEWS_ARTICLES).sort_values(by=['Ticker', 'Date'])
mercato_temp = pd.read_csv(cfg.ALL_PRICE_DATA).sort_values(by=['Ticker', 'WeekEndingFriday'])
indicatori_temp = pd.read_csv(cfg.FMP_FINANCIALS).sort_values(by=['symbol', 'WeekEndingFriday'])
""" 
# questo dataset manca, appena viene creato lo aggiungo
# risultati = pd.read_csv(cfg.NEWS_ARTICLES)

# per esempio e costuire anche il restante codice, creo un dataset fittizzio
"""
risultati_temp = pd.DataFrame({
    'Ticker': ['AAPL', 'MSFT'],
    'WeekEndingFriday': ['2020-01-03', '2020-01-03'],
    'result': ['Up', 'Down'],
    'prediction': ['Up', 'Down'],
    'probability_up': [0.8, 0.3],
    'probability_down': [0.2, 0.7]
})

# convertiamo le date in formato datetime, in modo da poter poi andare a popolare la tabella delle date del database relazionale.
aziende_temp['selectionReferenceDate'] = pd.to_datetime(mercato_temp['WeekEndingFriday']).dt.date 
mercato_temp['WeekEndingFriday'] = pd.to_datetime(mercato_temp['WeekEndingFriday']).dt.date
articoli_temp['Date'] = pd.to_datetime(articoli_temp['Date']).dt.date
indicatori_temp['WeekEndingFriday'] = pd.to_datetime(indicatori_temp['WeekEndingFriday']).dt.date
risultati_temp['WeekEndingFriday'] = pd.to_datetime(risultati_temp['WeekEndingFriday']).dt.date

#%%
"""
nomi_aziende = aziende_temp[['companyName']].drop_duplicates().reset_index(drop=True)
ticker_aziende = aziende_temp[['Ticker']].drop_duplicates().reset_index(drop=True)
"""
nomi_settori = aziende_temp[['sector', 'SectorCode']].drop_duplicates().reset_index(drop=True)
nomi_risultati = pd.DataFrame({'result': ['Up', 'Down'], 'label': [1, 0]})
nomi_industie = aziende_temp[['industry']].drop_duplicates().reset_index(drop=True)
calendario = pd.DataFrame()
"""
# creiamo la tabella in cui mettiamo i nomi delle aziende, con un id progressivo che ci servirà poi per popolare le tabelle del dataset relazionale 
# Aggiungi la colonna progressiva (partendo da 1)
nomi_aziende['Id_nomi_aziende'] = range(1, len(nomi_aziende) + 1)

# Riordina le colonne per avere l'ID all'inizio
nomi_aziende = nomi_aziende[['Id_nomi_aziende', 'companyName']]

# creiamo la tabella in cui mettiamo i ticker delle aziende, con un id progressivo che ci servirà poi per popolare le tabelle del dataset relazionale 
# Aggiungi la colonna progressiva (partendo da 1)
ticker_aziende['Id_ticker_aziende'] = range(1, len(ticker_aziende) + 1)

# Riordina le colonne per avere l'ID all'inizio
ticker_aziende = ticker_aziende[['Id_ticker_aziende', 'Ticker']]
"""
# creiamo la tabella in cui mettiamo i nomi del settore, con un id progressivo che ci servirà poi per popolare le tabelle del dataset relazionale 
# Aggiungi la colonna progressiva (partendo da 1)
nomi_settori['Id_nomi_settori'] = range(1, len(nomi_settori) + 1)

# Riordina le colonne per avere l'ID all'inizio
nomi_settori = nomi_settori[['Id_nomi_settori', 'sector', 'SectorCode']]

# creiamo la tabella in cui mettiamo i nomi dei risultati, con un id progressivo che ci servirà poi per popolare le tabelle del dataset relazionale
# Aggiungi la colonna progressiva (partendo da 1)
nomi_risultati['Id_nomi_risultati'] = range(1, len(nomi_risultati) + 1)

# Riordina le colonne per avere l'ID all'inizio
nomi_risultati = nomi_risultati[['Id_nomi_risultati', 'result', 'label']]


# definiamo un calendario con tutte le date comprese tra il 1 gennaio 2020 e oggi, in modo da poter poi andare a popolare la tabella delle date del database relazionale.
# Definiamo l'intervallo temporale
data_inizio = date(2020, 1, 1)
data_fine = date.today()

# Calcoliamo il numero di giorni totali
giorni_totali = (data_fine - data_inizio).days

# Generiamo la lista di tutte le date con il formato YYYY-DD-MM
calendario = pd.DataFrame({
    'date': [
        (data_inizio + timedelta(days=i)).strftime('%Y-%d-%m') 
        for i in range(giorni_totali + 1)
    ]
})
# creiamo la tabella con le date, con un id progressivo che ci servirà poi per popolare le tabelle del dataset relazionale
calendario['Id_calendario'] = range(1, len(calendario) + 1)
calendario = calendario[['Id_calendario', 'date']]
calendario['date'] = pd.to_datetime(calendario['date'], format='%Y-%d-%m').dt.date

# %%
# ora creiamo le chiavi primarie per le tabelle che abbiamo appena creato, in modo da poter poi andare a popolare le tabelle del database relazionale.
# 1. Creiamo la colonna con il numero progressivo (partendo da 1)
aziende = aziende_temp  # Creiamo una copia del DataFrame per lavorarci sopra
aziende['id_azienda'] = range(1, len(aziende) + 1)

# 2. La "stacchiamo" dal fondo e la mettiamo in prima posizione (indice 0)
colonna_id = aziende.pop('id_azienda') 
aziende.insert(0, 'id_azienda', colonna_id)

# Ora 'id_azienda' è la prima colonna

# 1. Creiamo la colonna con il numero progressivo (partendo da 1)
mercato = mercato_temp  # Creiamo una copia del DataFrame per lavorarci sopra
mercato['id_mercato'] = range(1, len(mercato) + 1)

# 2. La "stacchiamo" dal fondo e la mettiamo in prima posizione (indice 0)
colonna_id = mercato.pop('id_mercato') 
mercato.insert(0, 'id_mercato', colonna_id)

# Ora 'id_mercato' è la prima colonna

# 1. Creiamo la colonna con il numero progressivo (partendo da 1)
articoli = articoli_temp  # Creiamo una copia del DataFrame per lavorarci sopra
articoli['id_articoli'] = range(1, len(articoli) + 1)

# 2. La "stacchiamo" dal fondo e la mettiamo in prima posizione (indice 0)
colonna_id = articoli.pop('id_articoli') 
articoli.insert(0, 'id_articoli', colonna_id)

# Ora 'id_articoli' è la prima colonna

# 1. Creiamo la colonna con il numero progressivo (partendo da 1)
indicatori = indicatori_temp  # Creiamo una copia del DataFrame per lavorarci sopra
indicatori['id_indicatori'] = range(1, len(indicatori) + 1)

# 2. La "stacchiamo" dal fondo e la mettiamo in prima posizione (indice 0)
colonna_id = indicatori.pop('id_indicatori') 
indicatori.insert(0, 'id_indicatori', colonna_id)

# Ora 'id_indicatori' è la prima colonna

# 1. Creiamo la colonna con il numero progressivo (partendo da 1)
risultati = risultati_temp  # Creiamo una copia del DataFrame per lavorarci sopra
risultati['id_risultati'] = range(1, len(risultati) + 1)

# 2. La "stacchiamo" dal fondo e la mettiamo in prima posizione (indice 0)
colonna_id = risultati.pop('id_risultati') 
risultati.insert(0, 'id_risultati', colonna_id)

# Ora 'id_risultati' è la prima colonna

#%%
# ora creiamo delle funzioni grazie alle quali possiamo creare delle chiavi esterne per le tabelle mercato, articoli e indicatori, in modo da poter poi andare a popolare le tabelle del database relazionale.

def chiave_esterna_mercato(x):
    # Uso .values[0] per estrarre il numero pulito
    return aziende['id_azienda'][aziende['Ticker'] == x['Ticker']].item()

def chiave_esterna_articoli(x):
    return aziende['id_azienda'][aziende['Ticker'] == x['Ticker']].item()

def chiave_esterna_indicatori(x):
    # ATTENZIONE: qui uso x['symbol'] perché il dataset indicatori usa 'symbol'
    return aziende['id_azienda'][aziende['Ticker'] == x['symbol']].item()

def chiave_esterna_risultati(x):
    return aziende['id_azienda'][aziende['Ticker'] == x['Ticker']].item()

mercato['id_azienda'] = mercato.apply(chiave_esterna_mercato, axis=1)
articoli['id_azienda'] = articoli.apply(chiave_esterna_articoli, axis=1)
indicatori['id_azienda'] = indicatori.apply(chiave_esterna_indicatori, axis=1)
risultati['id_azienda'] = risultati.apply(chiave_esterna_risultati, axis=1)

# %%
# qui creiamo una serie di funzioni che ci permettono di andare a popolare le tabelle del database relazionale, andando a sostituire i nomi delle aziende, dei ticker, dei settori e delle date con i rispettivi id che abbiamo creato nelle tabelle che abbiamo appena creato.

def tabella_aziende(x):
    # Corretti i typo (Tricker/Triker -> Ticker) e i nomi degli ID
    """    
    x['Ticker'] = ticker_aziende['Id_ticker_aziende'][ticker_aziende['Ticker'] == x['Ticker']].item()
    x['companyName'] = nomi_aziende['Id_nomi_aziende'][nomi_aziende['companyName'] == x['companyName']].item()
    """ 
    x['sector'] = nomi_settori['Id_nomi_settori'][nomi_settori['sector'] == x['sector']].item()
    x['selectionReferenceDate'] = calendario['Id_calendario'][calendario['date'] == x['selectionReferenceDate']].item()
    return x

def tabella_mercato(x):
    """
    x['Ticker'] = ticker_aziende['Id_ticker_aziende'][ticker_aziende['Ticker'] == x['Ticker']].item()
    """
    x['WeekEndingFriday'] = calendario['Id_calendario'][calendario['date'] == x['WeekEndingFriday']].item()
    return x

def tabella_articoli(x):
    """
    x['Ticker'] = ticker_aziende['Id_ticker_aziende'][ticker_aziende['Ticker'] == x['Ticker']].item()
    """
    x['Date'] = calendario['Id_calendario'][calendario['date'] == x['Date']].item()
    return x

def tabella_indicatori(x):
    # Corretto in symbol
    """
    x['symbol'] = ticker_aziende['Id_ticker_aziende'][ticker_aziende['Ticker'] == x['symbol']].item()
    """
    x['WeekEndingFriday'] = calendario['Id_calendario'][calendario['date'] == x['WeekEndingFriday']].item()
    return x
def tabella_risultati(x):
    """
    x['Ticker'] = ticker_aziende['Id_ticker_aziende'][ticker_aziende['Ticker'] == x['Ticker']].item()
    """
    x['WeekEndingFriday'] = calendario['Id_calendario'][calendario['date'] == x['WeekEndingFriday']].item()
    x['result'] = nomi_risultati['Id_nomi_risultati'][nomi_risultati['result'] == x['result']].item()
    x['prediction'] = nomi_risultati['Id_nomi_risultati'][nomi_risultati['result'] == x['prediction']].item()
    return x

# Applicazione (questo richiederà un po' di tempo se i dataset sono grandi)
aziende = aziende.apply(tabella_aziende, axis=1)
mercato = mercato.apply(tabella_mercato, axis=1)
articoli = articoli.apply(tabella_articoli, axis=1)
indicatori = indicatori.apply(tabella_indicatori, axis=1)
risultati = risultati.apply(tabella_risultati, axis=1)
# %%
print(aziende.head())
print(mercato.head())
print(articoli.head())
print(indicatori.head())

# %%
