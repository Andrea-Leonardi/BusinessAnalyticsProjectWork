#%%
"""
qui prepariamo i dataset e le relative tabelle che poi usiamo per andare a rimepire il database relazionale.
"""
import pandas as pd
import sys 
import numpy as np
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 
from datetime import date, timedelta

aziende_temp = pd.read_csv(cfg.ENT).sort_values(by='Ticker')
articoli_temp = pd.read_csv(cfg.NEWS_ARTICLES).sort_values(by=['Ticker', 'Date'])
mercato_temp = pd.read_csv(cfg.ALL_PRICE_DATA).sort_values(by=['Ticker', 'WeekEndingFriday'])
indicatori_temp = pd.read_csv(cfg.FMP_FINANCIALS).sort_values(by=['symbol', 'WeekEndingFriday'])
granger_temp = pd.read_csv(cfg.GRANGER_LAG1_LAG2).sort_values(by=['Ticker', 'Sector'])
best_model = pd.read_csv(cfg.EVALUATION_BEST_MODELS_SUMMARY)
FinBert = pd.read_csv(cfg.MODELING_DATASET).sort_values(by=['Ticker', 'WeekEndingFriday'])[['Ticker', 'WeekEndingFriday', 'NEWS_Sentiment_Mean', 'NEWS_FINBERT_Granger_Score']]

# qui aggiungimo i dataset in cui sono presenti i risultati delle previsioni del modello, in modo da poter poi andare a popolare la tabella dei risultati del database relazionale.
# però essendo 11 i dataset, prima gli uniamo e poi a ciascuno aggiungiamo una colonna che indica il modello migliore attraverso cui è stata fatta la previsione.

BM = pd.read_csv(cfg.EVALUATION_BASIC_MATERIALS).sort_values(by='Ticker')
BM[['sector', 'best_model']] = best_model.iloc[0,0:2].tolist()
CS = pd.read_csv(cfg.EVALUATION_COMMUNICATION_SERVICES).sort_values(by='Ticker')
CS[['sector', 'best_model']] = best_model.iloc[1,0:2].tolist()
CC = pd.read_csv(cfg.EVALUATION_CONSUMER_CYCLICAL).sort_values(by='Ticker')
CC[['sector', 'best_model']] = best_model.iloc[2,0:2].tolist()
CD = pd.read_csv(cfg.EVALUATION_CONSUMER_DEFENSIVE).sort_values(by='Ticker')
CD[['sector', 'best_model']] = best_model.iloc[3,0:2].tolist()
E = pd.read_csv(cfg.EVALUATION_ENERGY).sort_values(by='Ticker')
E[['sector', 'best_model']] = best_model.iloc[4,0:2].tolist()
FS = pd.read_csv(cfg.EVALUATION_FINANCIAL_SERVICES).sort_values(by='Ticker')
FS[['sector', 'best_model']] = best_model.iloc[5,0:2].tolist()
H = pd.read_csv(cfg.EVALUATION_HEALTHCARE).sort_values(by='Ticker')
H[['sector', 'best_model']] = best_model.iloc[6,0:2].tolist()
I = pd.read_csv(cfg.EVALUATION_INDUSTRIALS).sort_values(by='Ticker')
I[['sector', 'best_model']] = best_model.iloc[7,0 :2].tolist()
RE = pd.read_csv(cfg.EVALUATION_REAL_ESTATE).sort_values(by='Ticker')
RE[['sector', 'best_model']] = best_model.iloc[8,0:2].tolist()
T = pd.read_csv(cfg.EVALUATION_TECHNOLOGY).sort_values(by='Ticker')
T[['sector', 'best_model']] = best_model.iloc[9,0:2].tolist()
U = pd.read_csv(cfg.EVALUATION_UTILITIES).sort_values(by='Ticker')
U[['sector', 'best_model']] = best_model.iloc[10,0:2].tolist()
risultati_temp = pd.concat([BM, CS, CC, CD, E, FS, H, I, RE, T, U], ignore_index=True)
risultati_temp.rename(columns={'predicted_AdjClosePrice_t+1_Up': 'predicted_AdjClosePrice_t+1', 'AdjClosePrice_t+1_Up': 'AdjClosePrice_t+1'}, inplace=True)

""" 
# questo dataset manca, appena viene creato lo aggiungo
# risultati = pd.read_csv(cfg.NEWS_ARTICLES)

# per esempio e costuire anche il restante codice, creo un dataset fittizzio
risultati_temp = pd.DataFrame({
    'Ticker': ['AAPL', 'MSFT'],
    'WeekEndingFriday': ['2020-01-03', '2020-01-03'],
    'result': ['Up', 'Down'],
    'prediction': ['Up', 'Down'],
    'probability_up': [0.8, 0.3],
    'probability_down': [0.2, 0.7]
})
"""

# convertiamo le date in formato datetime, in modo da poter poi andare a popolare la tabella delle date del database relazionale.
aziende_temp['selectionReferenceDate'] = pd.to_datetime(mercato_temp['WeekEndingFriday']).dt.date 
mercato_temp['WeekEndingFriday'] = pd.to_datetime(mercato_temp['WeekEndingFriday']).dt.date
articoli_temp['Date'] = pd.to_datetime(articoli_temp['Date']).dt.date
indicatori_temp['WeekEndingFriday'] = pd.to_datetime(indicatori_temp['WeekEndingFriday']).dt.date
risultati_temp['WeekEndingFriday'] = pd.to_datetime(risultati_temp['WeekEndingFriday']).dt.date
FinBert['WeekEndingFriday'] = pd.to_datetime(FinBert['WeekEndingFriday']).dt.date


# prendiamo il dataset Modeling, da cui vogliamo ottenere i coefficienti ottenuti attraverso la combinazione lineare 
# tra i valori di FinBert e i valori ottenuti dalla causalità di Granger, in modo poi da inserirli nella tabella indicatori,
# ma avendo i due dataframe una dimensione diversa, come prima cosa creiamo una funzione al fine di ottenere qui coefficienti solo
# per quelle righe che sono presneti anche nel dataset Indicatori

def estrai_coefficiente(x):
    # Creiamo la maschera per confrontare la riga x con TUTTO il DataFrame FinBert
    # Nota: usa x['symbol'] o x['Ticker'] a seconda del nome in indicatori_temp
    condizione = (FinBert['Ticker'] == x['symbol']) & \
                 (FinBert['WeekEndingFriday'] == x['WeekEndingFriday'])
    
    # Filtriamo FinBert per ottenere le colonne desiderate
    valori = FinBert.loc[condizione, ['NEWS_FINBERT_Granger_Score', 'NEWS_Sentiment_Mean']]
    
    if not valori.empty:
        # Se trova la riga, restituisce la prima occorrenza
        return valori.iloc[0]
    else:
        # Se non trova nulla, restituisce NaN per entrambe le colonne
        return pd.Series([np.nan, np.nan], index=['NEWS_FINBERT_Granger_Score', 'NEWS_Sentiment_Mean'])

# Applichiamo la funzione riga per riga
indicatori_temp[['NEWS_FINBERT_Granger_Score', 'NEWS_Sentiment_Mean']] = indicatori_temp.apply(estrai_coefficiente, axis=1)

#%%
"""
nomi_aziende = aziende_temp[['companyName']].drop_duplicates().reset_index(drop=True)
ticker_aziende = aziende_temp[['Ticker']].drop_duplicates().reset_index(drop=True)
"""
nomi_settori = aziende_temp[['sector', 'SectorCode']].drop_duplicates().reset_index(drop=True)
nomi_risultati = pd.DataFrame({'result': ['Up', 'Down'], 'label': [1, 0]})
nomi_industie = aziende_temp[['industry']].drop_duplicates().reset_index(drop=True)
nomi_modelli = best_model
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
nomi_industie['id_nomi_industie'] = range(1, len(nomi_industie) + 1)

# Riordina le colonne per avere l'ID all'inizio
nomi_industie = nomi_industie[['id_nomi_industie', 'industry']]

# creiamo la tabella in cui mettiamo i nomi del settore, con un id progressivo che ci servirà poi per popolare le tabelle del dataset relazionale 
# Aggiungi la colonna progressiva (partendo da 1)
nomi_settori['id_nomi_settori'] = range(1, len(nomi_settori) + 1)

# Riordina le colonne per avere l'ID all'inizio
nomi_settori = nomi_settori[['id_nomi_settori', 'sector', 'SectorCode']]

# creiamo la tabella in cui mettiamo i nomi dei risultati, con un id progressivo che ci servirà poi per popolare le tabelle del dataset relazionale
# Aggiungi la colonna progressiva (partendo da 1)
nomi_risultati['id_nomi_risultati'] = range(1, len(nomi_risultati) + 1)

# Riordina le colonne per avere l'ID all'inizio
nomi_risultati = nomi_risultati[['id_nomi_risultati', 'result', 'label']]

# creiamo la tabella in cui mettiamo i nomi del settore, con un id progressivo che ci servirà poi per popolare le tabelle del dataset relazionale 
# Aggiungi la colonna progressiva (partendo da 1)
nomi_modelli['id_nomi_modelli'] = range(1, len(nomi_modelli) + 1)

# Riordina le colonne per avere l'ID all'inizio
nomi_modelli = nomi_modelli[['id_nomi_modelli', 'best_model', 'test_accuracy', 'delta_null_model', 'delta_always_one', 'delta_always_zero', 'sector']]

# definiamo un calendario con tutte le date comprese tra il 1 gennaio 2010 e oggi, in modo da poter poi andare a popolare la tabella delle date del database relazionale.
# Definiamo l'intervallo temporale
data_inizio = date(2010, 1, 1)
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
calendario['id_calendario'] = range(1, len(calendario) + 1)
calendario = calendario[['id_calendario', 'date']]
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

# 1. Creiamo la colonna con il numero progressivo (partendo da 1)
granger = granger_temp  # Creiamo una copia del DataFrame per lavorarci sopra
granger['id_granger'] = range(1, len(granger) + 1)

# 2. La "stacchiamo" dal fondo e la mettiamo in prima posizione (indice 0)
colonna_id = granger.pop('id_granger') 
granger.insert(0, 'id_granger', colonna_id)

# Ora 'id_granger' è la prima colonna

#%%
# ora creiamo delle funzioni grazie alle quali possiamo creare delle chiavi esterne per le tabelle mercato, articoli, indicatori e granger, in modo da poter poi andare a popolare le tabelle del database relazionale.

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

def chiave_esterna_granger(x):
    return aziende['id_azienda'][aziende['Ticker'] == x['Ticker']].item()

mercato['id_azienda'] = mercato.apply(chiave_esterna_mercato, axis=1)
articoli['id_azienda'] = articoli.apply(chiave_esterna_articoli, axis=1)
indicatori['id_azienda'] = indicatori.apply(chiave_esterna_indicatori, axis=1)
risultati['id_azienda'] = risultati.apply(chiave_esterna_risultati, axis=1)
granger['id_azienda'] = granger.apply(chiave_esterna_granger, axis=1)
possibbili_risultati = nomi_risultati 


# dopo aver creato le chiavi esterne, possiamo eliminare le colonne che contengono i nomi delle aziende, dei ticker, dei settori e delle date, in quanto non ci serviranno più per popolare le tabelle del database relazionale.
mercato.drop(columns=['Ticker'], inplace=True)
articoli.drop(columns=['Ticker'], inplace=True)
indicatori.drop(columns=['symbol'], inplace=True)
risultati.drop(columns=['Ticker'], inplace=True)
granger.drop(columns=['Ticker'], inplace=True)
# %%
# qui creiamo una serie di funzioni che ci permettono di andare a popolare le tabelle del database relazionale, andando a sostituire i nomi delle aziende, dei ticker, dei settori e delle date con i rispettivi id che abbiamo creato nelle tabelle che abbiamo appena creato.

def tabella_aziende(x):
    # Corretti i typo (Tricker/Triker -> Ticker) e i nomi degli ID
    """    
    x['Ticker'] = ticker_aziende['Id_ticker_aziende'][ticker_aziende['Ticker'] == x['Ticker']].item()
    x['companyName'] = nomi_aziende['Id_nomi_aziende'][nomi_aziende['companyName'] == x['companyName']].item()
    """ 
    x['sector'] = nomi_settori['id_nomi_settori'][nomi_settori['sector'] == x['sector']].item()
    # x['selectionReferenceDate'] = calendario['id_calendario'][calendario['date'] == x['selectionReferenceDate']].item()
    x['industry'] = nomi_industie['id_nomi_industie'][nomi_industie['industry'] == x['industry']].item()
    return x

def tabella_mercato(x):
    """
    x['Ticker'] = ticker_aziende['Id_ticker_aziende'][ticker_aziende['Ticker'] == x['Ticker']].item()
    """
    x['WeekEndingFriday'] = calendario['id_calendario'][calendario['date'] == x['WeekEndingFriday']].item()
    return x

def tabella_articoli(x):
    """
    x['Ticker'] = ticker_aziende['Id_ticker_aziende'][ticker_aziende['Ticker'] == x['Ticker']].item()
    """
    x['Date'] = calendario['id_calendario'][calendario['date'] == x['Date']].item()
    return x

def tabella_indicatori(x):
    # Corretto in symbol
    """
    x['symbol'] = ticker_aziende['Id_ticker_aziende'][ticker_aziende['Ticker'] == x['symbol']].item()
    """
    x['WeekEndingFriday'] = calendario['id_calendario'][calendario['date'] == x['WeekEndingFriday']].item()
    return x
def tabella_risultati(x):
    """
    x['Ticker'] = ticker_aziende['Id_ticker_aziende'][ticker_aziende['Ticker'] == x['Ticker']].item()
    """
    x['WeekEndingFriday'] = calendario['id_calendario'][calendario['date'] == x['WeekEndingFriday']].item()
    x['AdjClosePrice_t+1'] = nomi_risultati['id_nomi_risultati'][nomi_risultati['label'] == x['AdjClosePrice_t+1']].item()
    x['predicted_AdjClosePrice_t+1'] = nomi_risultati['id_nomi_risultati'][nomi_risultati['label'] == x['predicted_AdjClosePrice_t+1']].item()
    # 1. Creiamo una maschera booleana: True dove ENTRAMBE le colonne coincidono
    mask = (nomi_modelli['sector'] == x['sector']) & (nomi_modelli['best_model'] == x['best_model'])
    
    # 2. Estraiamo l'ID corrispondente usando .item() come nelle altre righe
    x['best_model'] = nomi_modelli.loc[mask, 'id_nomi_modelli'].item()
    return x

def tabella_granger(x):
    """
    x['Ticker'] = ticker_aziende['Id_ticker_aziende'][ticker_aziende['Ticker'] == x['Ticker']].item()
    """
    x['Sector'] = nomi_settori['id_nomi_settori'][nomi_settori['sector'] == x['Sector']].item()
    return x

# Applicazione (questo richiederà un po' di tempo se i dataset sono grandi)
aziende = aziende.apply(tabella_aziende, axis=1)
mercato = mercato.apply(tabella_mercato, axis=1)
articoli = articoli.apply(tabella_articoli, axis=1)
indicatori = indicatori.apply(tabella_indicatori, axis=1)
risultati = risultati.apply(tabella_risultati, axis=1)
granger = granger.apply(tabella_granger, axis=1)
#%% 
# dopo aver creato le chiavi esterne, possiamo rinominare le colonne per riflettere i nuovi nomi degli ID
aziende.rename(columns={'sector': 'id_settore', 'selectionReferenceDate': 'id_calendario', 'industry': 'id_industry'}, inplace=True)
mercato.rename(columns={'WeekEndingFriday': 'id_calendario'}, inplace=True)
articoli.rename(columns={'Date': 'id_calendario', 'ID': 'id_articoli_originali'}, inplace=True)
indicatori.rename(columns={'WeekEndingFriday': 'id_calendario'}, inplace=True)
risultati.rename(columns={'WeekEndingFriday': 'id_calendario', 'best_model': 'id_best_model'}, inplace=True)
possibbili_risultati.rename(columns={'id_nomi_risultati': 'id_result'}, inplace=True)
granger.rename(columns={'Sector': 'id_nomi_settori'}, inplace=True)
nomi_modelli.rename(columns={'id_nomi_modelli': 'id_best_model'}, inplace=True)
risultati.drop(columns=['sector'], inplace=True)
nomi_modelli.drop(columns=['sector'], inplace=True)

# riordiniamo le colonne per avere come prima cosa la chiave interna, poi la chiave esterna, e poi le altre colonne
# Riordino Tabella Aziende
aziende = aziende[['id_azienda', 'id_settore', 'id_industry', 'Ticker', 'companyName', 'SectorCode', 'marketCap', 'historicalMarketCapDate', 'universeSource']]

# Riordino Tabella Mercato
mercato = mercato[['id_mercato', 'id_azienda', 'id_calendario', 'ClosePrice', 'ClosePrice_t-1', 'ClosePrice_t-2', 'ClosePrice_t+1', 'AdjClosePrice', 'AdjClosePrice_t-1', 'AdjClosePrice_t-2', 'AdjClosePrice_t+1', 'AdjClosePrice_t+1_Up', 'WeeklyReturn_1W', 'WeeklyReturn_4W', 'Momentum_12W', 'Volatility_4W', 'Volatility_12W', 'Drawdown_12W']]

# Riordino Tabella Articoli
# Nota: id_calendario non era presente nei tuoi dati originali, assicurati di averlo creato prima di questa riga
articoli = articoli[['id_articoli', 'id_articoli_originali', 'id_azienda', 'id_calendario', 'Headline', 'Summary']]

# Riordino Tabella Indicatori
indicatori = indicatori[['id_indicatori', 'id_azienda', 'id_calendario', 'company_name', 'QuarterlyReleased', 'BookToMarket', 'MarketCap', 'FreeCashFlowYield', 'FreeCashFlowYield_TTM', 'EarningsYield', 'EarningsYield_TTM', 'BookToMarket_L1W', 'MarketCap_L1W', 'FreeCashFlowYield_L1W', 'FreeCashFlowYield_TTM_L1W', 'EarningsYield_L1W', 'EarningsYield_TTM_L1W', 'BookToMarket_L2W', 'MarketCap_L2W', 'FreeCashFlowYield_L2W', 'FreeCashFlowYield_TTM_L2W', 'EarningsYield_L2W', 'EarningsYield_TTM_L2W', 'GrossProfitability', 'GrossProfitability_TTM', 'OperatingMargin', 'OperatingMargin_TTM', 'ROA', 'ROA_TTM', 'AssetGrowth', 'InvestmentIntensity', 'Accruals', 'Accruals_TTM', 'DebtToAssets', 'WorkingCapitalScaled', 'GrossProfitability_L1Q', 'GrossProfitability_TTM_L1Q', 'OperatingMargin_L1Q', 'OperatingMargin_TTM_L1Q', 'ROA_L1Q', 'ROA_TTM_L1Q', 'AssetGrowth_L1Q', 'InvestmentIntensity_L1Q', 'Accruals_L1Q', 'Accruals_TTM_L1Q', 'DebtToAssets_L1Q', 'WorkingCapitalScaled_L1Q', 'GrossProfitability_L2Q', 'GrossProfitability_TTM_L2Q', 'OperatingMargin_L2Q', 'OperatingMargin_TTM_L2Q', 'ROA_L2Q', 'ROA_TTM_L2Q', 'AssetGrowth_L2Q', 'InvestmentIntensity_L2Q', 'Accruals_L2Q', 'Accruals_TTM_L2Q', 'DebtToAssets_L2Q', 'WorkingCapitalScaled_L2Q', 'NEWS_FINBERT_Granger_Score', 'NEWS_Sentiment_Mean']]

# Riordino Tabella Risultati
risultati = risultati[['id_risultati', 'id_azienda', 'id_calendario', 'id_best_model', 'AdjClosePrice_t+1', 'predicted_AdjClosePrice_t+1', 'predicted_probability']]

# Riordino Tabella Granger
granger = granger[['id_granger', 'id_azienda', 'id_nomi_settori', 'L1.AdjClosePrice_lag1', 'L1.NEWS_FINBERT_Negative_Mean_lag1', 'L1.NEWS_FINBERT_Positive_Mean_lag1', 'L1.NEWS_FINBERT_Neutral_Mean_lag1', 'pvalue_lag1', 'L1.AdjClosePrice_lag2', 'L1.NEWS_FINBERT_Negative_Mean_lag2', 'L1.NEWS_FINBERT_Positive_Mean_lag2', 'L1.NEWS_FINBERT_Neutral_Mean_lag2', 'L2.AdjClosePrice_lag2', 'L2.NEWS_FINBERT_Negative_Mean_lag2', 'L2.NEWS_FINBERT_Positive_Mean_lag2', 'L2.NEWS_FINBERT_Neutral_Mean_lag2', 'pvalue_lag2']]

# eliminiamo le colonne che contengono i nomi delle aziende, dei ticker, dei settori e delle date, in quanto non ci serviranno più per popolare le tabelle del database relazionale.
aziende.drop(columns=['SectorCode', 'marketCap', 'historicalMarketCapDate', 'universeSource'], inplace=True)
indicatori.drop(columns=['company_name'], inplace=True)

# %%
print("Colonne Aziende:", aziende.columns.tolist())
print("Colonne Mercato:", mercato.columns.tolist())
print("Colonne Articoli:", articoli.columns.tolist())
print("Colonne Indicatori:", indicatori.columns.tolist())
print("Colonne Risultati:", risultati.columns.tolist())
print("Colonne Calendario:", calendario.columns.tolist())
print("Colonne Possibili Risultati:", possibbili_risultati.columns.tolist())
print("Colonne Granger:", granger.columns.tolist())

# %%
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Date, ForeignKey, Text

# ==========================================
# 1. CONFIGURAZIONE CONNESSIONE
# ==========================================
# Sostituisci 'latuapassword' con quella scelta durante l'installazione
# Sostituisci 'db_progetto' con il nome che hai dato al database in pgAdmin
USER = 'postgres'
PASSWORD = 'Gorilla2026!' 
HOST = 'localhost'
PORT = '5432'
DB_NAME = 'project_business_analytics'

# Creazione della stringa di connessione
DATABASE_URI = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}'

# Creazione dell'engine
engine = create_engine(DATABASE_URI)
metadata = MetaData()

# ==========================================
# 2. DEFINIZIONE DELLO SCHEMA DELLE TABELLE
# ==========================================

# Tabelle Dimensionali (Tabelle di Lookup, senza chiavi esterne)
calendario_db = Table('calendario', metadata,
    Column('id_calendario', Integer, primary_key=True),
    Column('date', Date)
)

settori_db = Table('settori', metadata,
    Column('id_settore', Integer, primary_key=True),
    Column('sector', String(100)),
    Column('SectorCode', String(50))
)

industrie_db = Table('industrie', metadata,
    Column('id_industry', Integer, primary_key=True),
    Column('industry', String(100))
)

possibili_risultati_db = Table('possibili_risultati', metadata,
    Column('id_result', Integer, primary_key=True),
    Column('result', String(50)),
    Column('label', Integer)
)

modelli_db = Table('modelli', metadata,
    Column('id_best_model', Integer, primary_key=True),
    Column('best_model', String(100)),
    Column('test_accuracy', Float),
    Column('delta_null_model', Float),
    Column('delta_always_one', Float),
    Column('delta_always_zero', Float)
)

# Tabella Aziende (Contiene chiavi esterne verso settori e industrie)
aziende_db = Table('aziende', metadata,
    Column('id_azienda', Integer, primary_key=True),
    Column('id_settore', Integer, ForeignKey('settori.id_settore')),
    Column('id_industry', Integer, ForeignKey('industrie.id_industry')),
    Column('Ticker', String(10)),
    Column('companyName', String(200))
)

# Tabelle dei Dati (Fatti) che puntano ad Azienda e Calendario
mercato_db = Table('mercato', metadata,
    Column('id_mercato', Integer, primary_key=True),
    Column('id_azienda', Integer, ForeignKey('aziende.id_azienda')),
    Column('id_calendario', Integer, ForeignKey('calendario.id_calendario')),
    # Tutte le altre colonne numeriche come Float (Decimali)
    Column('ClosePrice', Float), Column('ClosePrice_t-1', Float), Column('ClosePrice_t-2', Float), 
    Column('ClosePrice_t+1', Float), Column('AdjClosePrice', Float), Column('AdjClosePrice_t-1', Float), 
    Column('AdjClosePrice_t-2', Float), Column('AdjClosePrice_t+1', Float), Column('AdjClosePrice_t+1_Up', Float), 
    Column('WeeklyReturn_1W', Float), Column('WeeklyReturn_4W', Float), Column('Momentum_12W', Float), 
    Column('Volatility_4W', Float), Column('Volatility_12W', Float), Column('Drawdown_12W', Float)
)

articoli_db = Table('articoli', metadata,
    Column('id_articoli', Integer, primary_key=True),
    Column('id_articoli_originali', String(100)), # Testo o numero in base al dataset originale
    Column('id_azienda', Integer, ForeignKey('aziende.id_azienda')),
    Column('id_calendario', Integer, ForeignKey('calendario.id_calendario')),
    Column('Headline', Text), # Text per stringhe molto lunghe
    Column('Summary', Text)
)

# Aggiungo la definizione dinamica delle colonne per Indicatori (sono tantissime, tutte float tranne le chiavi)
colonne_indicatori = [
    Column('id_indicatori', Integer, primary_key=True),
    Column('id_azienda', Integer, ForeignKey('aziende.id_azienda')),
    Column('id_calendario', Integer, ForeignKey('calendario.id_calendario'))
]
# Prendo i nomi delle metriche dalla tua lista ignorando i primi 3 campi che ho già definito
nomi_metriche_ind = ['QuarterlyReleased', 'BookToMarket', 'MarketCap', 'FreeCashFlowYield', 'FreeCashFlowYield_TTM', 'EarningsYield', 'EarningsYield_TTM', 'BookToMarket_L1W', 'MarketCap_L1W', 'FreeCashFlowYield_L1W', 'FreeCashFlowYield_TTM_L1W', 'EarningsYield_L1W', 'EarningsYield_TTM_L1W', 'BookToMarket_L2W', 'MarketCap_L2W', 'FreeCashFlowYield_L2W', 'FreeCashFlowYield_TTM_L2W', 'EarningsYield_L2W', 'EarningsYield_TTM_L2W', 'GrossProfitability', 'GrossProfitability_TTM', 'OperatingMargin', 'OperatingMargin_TTM', 'ROA', 'ROA_TTM', 'AssetGrowth', 'InvestmentIntensity', 'Accruals', 'Accruals_TTM', 'DebtToAssets', 'WorkingCapitalScaled', 'GrossProfitability_L1Q', 'GrossProfitability_TTM_L1Q', 'OperatingMargin_L1Q', 'OperatingMargin_TTM_L1Q', 'ROA_L1Q', 'ROA_TTM_L1Q', 'AssetGrowth_L1Q', 'InvestmentIntensity_L1Q', 'Accruals_L1Q', 'Accruals_TTM_L1Q', 'DebtToAssets_L1Q', 'WorkingCapitalScaled_L1Q', 'GrossProfitability_L2Q', 'GrossProfitability_TTM_L2Q', 'OperatingMargin_L2Q', 'OperatingMargin_TTM_L2Q', 'ROA_L2Q', 'ROA_TTM_L2Q', 'AssetGrowth_L2Q', 'InvestmentIntensity_L2Q', 'Accruals_L2Q', 'Accruals_TTM_L2Q', 'DebtToAssets_L2Q', 'WorkingCapitalScaled_L2Q', 'NEWS_FINBERT_Granger_Score', 'NEWS_Sentiment_Mean']
for col in nomi_metriche_ind:
    colonne_indicatori.append(Column(col, Float))
indicatori_db = Table('indicatori', metadata, *colonne_indicatori)

# Tabella Risultati (Ha due chiavi esterne che puntano entrambe a possibili_risultati)
risultati_db = Table('risultati', metadata,
    Column('id_risultati', Integer, primary_key=True),
    Column('id_azienda', Integer, ForeignKey('aziende.id_azienda')),
    Column('id_calendario', Integer, ForeignKey('calendario.id_calendario')),
    Column('id_best_model', Integer, ForeignKey('modelli.id_best_model')), # <--- MODIFICATO QUI: punta a 'modelli'
    Column('AdjClosePrice_t+1', Integer, ForeignKey('possibili_risultati.id_result')), 
    Column('predicted_AdjClosePrice_t+1', Integer, ForeignKey('possibili_risultati.id_result')), 
    Column('predicted_probability', Float)
)

# --- NUOVA TABELLA: coefficiente_granger ---
colonne_granger = [
    Column('id_granger', Integer, primary_key=True),
    Column('id_azienda', Integer, ForeignKey('aziende.id_azienda')),
    Column('id_settore', Integer, ForeignKey('settori.id_settore'))
]



nomi_metriche_granger = [
    'L1.AdjClosePrice_lag1', 'L1.NEWS_FINBERT_Negative_Mean_lag1', 'L1.NEWS_FINBERT_Positive_Mean_lag1',
    'L1.NEWS_FINBERT_Neutral_Mean_lag1', 'pvalue_lag1', 'L1.AdjClosePrice_lag2',
    'L1.NEWS_FINBERT_Negative_Mean_lag2', 'L1.NEWS_FINBERT_Positive_Mean_lag2',
    'L1.NEWS_FINBERT_Neutral_Mean_lag2', 'L2.AdjClosePrice_lag2', 'L2.NEWS_FINBERT_Negative_Mean_lag2',
    'L2.NEWS_FINBERT_Positive_Mean_lag2', 'L2.NEWS_FINBERT_Neutral_Mean_lag2', 'pvalue_lag2'
]

# Aggiungo dinamicamente tutte le colonne dei lag e p-value come Float (Decimali)
for col in nomi_metriche_granger:
    colonne_granger.append(Column(col, Float))

coefficiente_granger_db = Table('coefficiente_granger', metadata, *colonne_granger)

# ==========================================
# 3. CREAZIONE TABELLE NEL DATABASE
# ==========================================
# 1. ELIMINA tutte le tabelle (se esistono) per evitare l'errore dei duplicati
metadata.drop_all(engine)

# 2. RICREA tutte le tabelle da zero e pulite
metadata.create_all(engine)
print("Tabelle create con successo nel database PostgreSQL.")

# ==========================================
# 4. POPOLAMENTO DELLE TABELLE (L'ordine è fondamentale per le Foreign Keys!)
# ==========================================
# ATTENZIONE: Assicurati che i nomi dei tuoi DataFrame e delle colonne siano ESATTAMENTE quelli dichiarati.
# (nomi_risultati nel tuo codice aveva 'id_nomi_risultati', lo rinominiamo per far match con il DB)
nomi_risultati.rename(columns={'id_nomi_risultati': 'id_result'}, inplace=True)

# 4.1 Tabelle indipendenti (Genitori)
calendario.to_sql('calendario', engine, if_exists='append', index=False)
nomi_settori.rename(columns={'id_nomi_settori': 'id_settore'}).to_sql('settori', engine, if_exists='append', index=False)
nomi_industie.rename(columns={'id_nomi_industie': 'id_industry'}).to_sql('industrie', engine, if_exists='append', index=False)
nomi_risultati.to_sql('possibili_risultati', engine, if_exists='append', index=False)
nomi_modelli.to_sql('modelli', engine, if_exists='append', index=False)

# 4.2 Tabella Intermedia
aziende.to_sql('aziende', engine, if_exists='append', index=False)

# 4.3 Tabelle dipendenti (Figli)
mercato.to_sql('mercato', engine, if_exists='append', index=False)
indicatori.to_sql('indicatori', engine, if_exists='append', index=False)
articoli.to_sql('articoli', engine, if_exists='append', index=False)
risultati.to_sql('risultati', engine, if_exists='append', index=False)

# Rinomino la colonna per allinearla alla Foreign Key definita nello schema ('id_settore')
granger.rename(columns={'id_nomi_settori': 'id_settore'}, inplace=True)

# Scrittura nel database
granger.to_sql('coefficiente_granger', engine, if_exists='append', index=False)

print("Caricamento dei dati nel database relazionale completato!")
# %%
# Query 1: Le 10 aziende con Prediction Probability più alta (Predizione = 0)
query_pred_0 = """
SELECT 
    a."companyName", 
    a."Ticker", 
    s.sector,
    c.date,
    pr_actual.label AS actual_value,
    pr_pred.label AS predicted_value,
    r.predicted_probability,
    m.best_model,
    m.test_accuracy,
    m.delta_null_model,
    m.delta_always_one,
    m.delta_always_zero
FROM risultati r
JOIN aziende a ON r.id_azienda = a.id_azienda
JOIN settori s ON a.id_settore = s.id_settore
JOIN calendario c ON r.id_calendario = c.id_calendario
JOIN modelli m ON r.id_best_model = m.id_best_model
JOIN possibili_risultati pr_actual ON r."AdjClosePrice_t+1" = pr_actual.id_result
JOIN possibili_risultati pr_pred ON r."predicted_AdjClosePrice_t+1" = pr_pred.id_result
WHERE pr_pred.label = 0
ORDER BY r.predicted_probability DESC
LIMIT 10;
"""

df_top10_pred_0 = pd.read_sql(query_pred_0, engine)
print("--- TOP 10 AZIENDE (PREDIZIONE = 0) ---")
print(df_top10_pred_0.to_string())


# Query 2: Le 10 aziende con Prediction Probability più alta (Predizione = 1)
query_pred_1 = """
SELECT 
    a."companyName", 
    a."Ticker", 
    s.sector,
    c.date,
    pr_actual.label AS actual_value,
    pr_pred.label AS predicted_value,
    r.predicted_probability,
    m.best_model,
    m.test_accuracy,
    m.delta_null_model,
    m.delta_always_one,
    m.delta_always_zero
FROM risultati r
JOIN aziende a ON r.id_azienda = a.id_azienda
JOIN settori s ON a.id_settore = s.id_settore
JOIN calendario c ON r.id_calendario = c.id_calendario
JOIN modelli m ON r.id_best_model = m.id_best_model
JOIN possibili_risultati pr_actual ON r."AdjClosePrice_t+1" = pr_actual.id_result
JOIN possibili_risultati pr_pred ON r."predicted_AdjClosePrice_t+1" = pr_pred.id_result
WHERE pr_pred.label = 1
ORDER BY r.predicted_probability DESC
LIMIT 10;
"""

df_top10_pred_1 = pd.read_sql(query_pred_1, engine)
print("\n--- TOP 10 AZIENDE (PREDIZIONE = 1) ---")
print(df_top10_pred_1.to_string())


# Query 3: Le 10 aziende con Prediction Probability più vicina a 0.5 (Incertezza massima)
# Per trovare i valori più vicini a 0.5, utilizziamo la funzione matematica valore assoluto ABS() 
# sulla differenza tra la probabilità e 0.5, e ordiniamo in modo crescente (i valori con differenza 
# minore sono i più vicini a 0.5).
query_incerti = """
SELECT 
    a."companyName", 
    a."Ticker", 
    s.sector,
    c.date,
    pr_actual.label AS actual_value,
    pr_pred.label AS predicted_value,
    r.predicted_probability,
    m.best_model,
    m.test_accuracy,
    m.delta_null_model,
    m.delta_always_one,
    m.delta_always_zero
FROM risultati r
JOIN aziende a ON r.id_azienda = a.id_azienda
JOIN settori s ON a.id_settore = s.id_settore
JOIN calendario c ON r.id_calendario = c.id_calendario
JOIN modelli m ON r.id_best_model = m.id_best_model
JOIN possibili_risultati pr_actual ON r."AdjClosePrice_t+1" = pr_actual.id_result
JOIN possibili_risultati pr_pred ON r."predicted_AdjClosePrice_t+1" = pr_pred.id_result
ORDER BY ABS(r.predicted_probability - 0.5) ASC
LIMIT 10;
"""

df_top10_incerti = pd.read_sql(query_incerti, engine)
print("\n--- TOP 10 AZIENDE CON MAGGIORE INCERTEZZA (~0.5) ---")
print(df_top10_incerti.to_string())





# %%
