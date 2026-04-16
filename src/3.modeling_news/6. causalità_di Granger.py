#%%
from statsmodels.tsa.stattools import grangercausalitytests
from statsmodels.tsa.vector_ar.var_model import VAR
import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
import sys 
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 

df_completo = pd.read_csv(cfg.MODELING_DATASET)
list_enterprises = df_completo["Ticker"].unique()
# %%
indice1 = df_completo.columns.get_loc("NEWS_TEXTBLOB_Polarity_Mean")
print(f"La colonna si trova all'indice: {indice1}")
indice2 = df_completo.columns.get_loc("NEWS_EMO_Neutral_Mean")
print(f"La colonna si trova all'indice: {indice2}")
numero_colonne = df_completo.shape[1]

# ordiniamo le colonne per nome e poi per data 
df_completo = df_completo.sort_values(by=['Ticker', 'WeekEndingFriday'], ascending=[True, True])

# Creiamo la lista degli indici: il numero 2 + il range da 4 a 68 (escluso)
colonne_da_togliere = [0] + [2] + [3] + list(range(5, indice1)) + list(range(indice2, numero_colonne))

# Usiamo df.columns per trasformare i numeri in nomi reali ed eliminarli
df = df_completo.drop(df_completo.columns[colonne_da_togliere], axis=1)
"""
questa è la parte di codice che deve essere modificata, e bisogna aggiungere il nome delle 6 colonne non presenti in questo momento    
im particolare bisogna incrementare a 3:14 piuttosto che 3:8
"""
# prendiamo i nomi delle clonne 
nomi_colonne = df.columns.tolist()
nomi_colonne = nomi_colonne[2:8] 
print(nomi_colonne)

"""
si deve modificare anche questa parte, aggiungendo nella seocnda lista il valore Granger_Nome_Libreria     
 """

# creiamo 11 nuove colonne a cui inizialmente attribuiamo dei valori mancanti
# Aggiunge il prefisso a ogni elemento
numero_lag = 5
# Parte Standard (Granger Bivariato)
# 1. Definiamo i suffissi che vogliamo per ogni combinazione
suffissi = ['pvalue', 'alpha', 'beta']

# 2. Parte standard
# Ciclo esterno: nome -> Ciclo medio: lag -> Ciclo interno: prefisso (cambia per primo)
lista_base = [
    f'Granger_{suf}_lag_{lag}_{nome}'
    for nome in nomi_colonne
    for lag in range(1, numero_lag + 1)
    for suf in suffissi
]

# 2. Parte specifica per TEXTBLOB (Beta 1 e Beta 2)
# Qui il prefisso cambia per primo, l'ordine è fisso
# Parte VAR TEXTBLOB (Lag 1, 2 e 3)
test_textblob = [
    f'Granger_{suf}_lag_{lag}_TEXTBLOB'
    for lag in [1, 2, 3]  # Esteso a 3
    for suf in ['pvalue', 'alpha', 'beta1', 'beta2']
]

# 3. Parte specifica per FINBERT (Beta 1, Beta 2, Beta 3)
# Parte VAR FINBERT (Lag 1, 2 e 3)
test_finbert = [
    f'Granger_{suf}_lag_{lag}_FINBERT'
    for lag in [1, 2, 3]
    for suf in ['pvalue', 'alpha', 'beta1', 'beta2', 'beta3']
]

# 4. Unione finale
nomi_nuove_colonne = lista_base + test_textblob + test_finbert

# Controllo dei primi elementi
print(nomi_nuove_colonne[:6])


# Crei le nuove colonne nel DataFrame e le riempi di NaN
df_granger = pd.DataFrame(df["Ticker"].unique(), columns=["Ticker"])
df_granger[nomi_nuove_colonne] = np.nan


# %%
def casualita_di_granger_riga(riga, lag_max):
    ticker = riga['Ticker']
    enterprises = df[df["Ticker"] == ticker]
    
    if len(enterprises) <= lag_max + 2:
        return riga

    # --- PARTE 1: GRANGER STANDARD (Bivariata) ---
    cols_sentiment = ['NEWS_TEXTBLOB_Polarity_Mean', 'NEWS_TEXTBLOB_Subjectivity_Mean', 
                      'NEWS_FINBERT_Negative_Mean', 'NEWS_FINBERT_Neutral_Mean', 'NEWS_FINBERT_Positive_Mean']

    for col_s in cols_sentiment:
        try:
            risultati = grangercausalitytests(enterprises[["AdjClosePrice", col_s]], maxlag=lag_max, verbose=False)
            for lg in range(1, lag_max + 1):
                res_l = risultati[lg]
                p_val = res_l[0]['ssr_ftest'][1]
                modello = res_l[1][0]
                
                # Alpha (Intercetta) e Beta (Sentiment)
                riga[f'Granger_pvalue_lag_{lg}_{col_s}'] = round(float(p_val), 7)
                riga[f'Granger_alpha_lag_{lg}_{col_s}'] = round(float(modello.params[0]), 7)
                riga[f'Granger_beta_lag_{lg}_{col_s}'] = round(float(modello.params[-1]), 7)
        except: continue

    # --- PARTE 2: VAR TEXTBLOB (Prezzo + Polarity + Subjectivity) ---
    try:
        # Fit del modello con 3 lag
        res_tb = VAR(enterprises[["AdjClosePrice", "NEWS_TEXTBLOB_Polarity_Mean", "NEWS_TEXTBLOB_Subjectivity_Mean"]]).fit(3)
        params_tb = res_tb.params['AdjClosePrice']
        
        for lg in [1, 2, 3]:
            # Il p-value nel VAR è solitamente riferito al test di significatività del coefficiente
            riga[f'Granger_pvalue_lag_{lg}_TEXTBLOB'] = round(float(res_tb.pvalues.loc[f'L{lg}.NEWS_TEXTBLOB_Polarity_Mean', 'AdjClosePrice']), 7)
            riga[f'Granger_alpha_lag_{lg}_TEXTBLOB'] = round(float(params_tb['const']), 7)
            riga[f'Granger_beta1_lag_{lg}_TEXTBLOB'] = round(float(params_tb[f'L{lg}.NEWS_TEXTBLOB_Polarity_Mean']), 7)
            riga[f'Granger_beta2_lag_{lg}_TEXTBLOB'] = round(float(params_tb[f'L{lg}.NEWS_TEXTBLOB_Subjectivity_Mean']), 7)
    except: pass

    # --- PARTE 3: VAR FINBERT (Prezzo + Neg + Neu + Pos) ---
    try:
        res_fb = VAR(enterprises[["AdjClosePrice", "NEWS_FINBERT_Negative_Mean", "NEWS_FINBERT_Neutral_Mean", "NEWS_FINBERT_Positive_Mean"]]).fit(3)
        params_fb = res_fb.params['AdjClosePrice']
        
        for lg in [1, 2, 3]:
            riga[f'Granger_pvalue_lag_{lg}_FINBERT'] = round(float(res_fb.pvalues.loc[f'L{lg}.NEWS_FINBERT_Negative_Mean', 'AdjClosePrice']), 7)
            riga[f'Granger_alpha_lag_{lg}_FINBERT'] = round(float(params_fb['const']), 7)
            riga[f'Granger_beta1_lag_{lg}_FINBERT'] = round(float(params_fb[f'L{lg}.NEWS_FINBERT_Negative_Mean']), 7)
            riga[f'Granger_beta2_lag_{lg}_FINBERT'] = round(float(params_fb[f'L{lg}.NEWS_FINBERT_Neutral_Mean']), 7)
            riga[f'Granger_beta3_lag_{lg}_FINBERT'] = round(float(params_fb[f'L{lg}.NEWS_FINBERT_Positive_Mean']), 7)
    except: pass

    return riga


# --- ESECUZIONE ---
df_granger = df_granger.apply(lambda x: casualita_di_granger_riga(x, numero_lag), axis=1)

df_granger.to_csv(cfg.DATA_GRANGER, index=False, encoding='utf-8-sig')

# %%
def trova_casi_significativi(df_risultati, soglia_alfa=0.05):
    """
    Trova e stampa le aziende e le colonne con p-value < soglia_alfa.
    """
    print(f"--- Casi significativi (Livello: {soglia_alfa}) ---")
    
    # Selezioniamo solo le colonne che contengono i p-value
    cols_pvalue = [c for c in df_risultati.columns if 'pvalue' in c]
    
    risultati_lista = []

    for index, riga in df_risultati.iterrows():
        ticker = riga['Ticker']
        for col in cols_pvalue:
            p_val = riga[col]
            # Verifichiamo se il valore non è NaN e se è sotto la soglia
            if pd.notna(p_val) and p_val < soglia_alfa:
                print(f"Azienda: {ticker} | Nomi Colonna: {col} | P-Value: {p_val}")
                risultati_lista.append({'Ticker': ticker, 'Nomi Colonna': col, 'P-Value': p_val})
                
    return pd.DataFrame(risultati_lista)


df_casi_significativi = trova_casi_significativi(df_granger, soglia_alfa=1)
df_casi_significativi = df_casi_significativi.sort_values(by="P-Value")

# rappresentiaom una tabella di frequenza dei nomi delle colonne ottenuti 
# Genera la tabella di frequenza per i tipi di test/colonne
frequenza_test = df_casi_significativi['Nomi Colonna'].value_counts().reset_index()

# Rinomina le colonne per rendere la tabella leggibile
frequenza_test.columns = ['Test_Granger_Colonna', 'Frequenza_Significativi']

# Visualizza i risultati ordinati (dai test più "predittivi" a quelli meno)
print(frequenza_test)



def conteggio_significativi_per_colonna(df_risultati, soglia_alfa=0.05):
    """
    Conta quanti p-value significativi ci sono per ogni colonna del dataset.
    """
    cols_pvalue = [c for c in df_risultati.columns if 'pvalue' in c]
    
    # Calcoliamo quanti valori in ogni colonna sono minori della soglia
    conteggi = (df_risultati[cols_pvalue] < soglia_alfa).sum()
    
    # Creiamo un DataFrame leggibile
    df_conteggio = conteggi.reset_index()
    df_conteggio.columns = ['Nome_Colonna', 'Numero_Significativi']
    
    # Ordiniamo per i più significativi
    return df_conteggio.sort_values(by='Numero_Significativi', ascending=False)


riepilogo_significativi = conteggio_significativi_per_colonna(df_granger, soglia_alfa=1)
print(riepilogo_significativi)


# %%

# ==============================================================================
# ==================== RAPPRESENTAZIONE GRAFICA DEFINITIVA =====================
# ==============================================================================
import matplotlib.dates as mdates
import numpy as np
import matplotlib.pyplot as plt

# --- PREPARAZIONE DATI GENERALE E RISOLUZIONE ERRORI ---
# 1. Recuperiamo la colonna della data che era stata eliminata dal dataset 'df'
df['WeekEndingFriday'] = df_completo['WeekEndingFriday']

# 2. Assicuriamoci che la colonna Ticker sia stringa ovunque
df['Ticker'] = df['Ticker'].astype(str)
df_completo['Ticker'] = df_completo['Ticker'].astype(str)
df_granger['Ticker'] = df_granger['Ticker'].astype(str)

# 3. Funzione condivisa per calcolare la Base 100 sulle azioni
def calcola_base_100_azioni(serie_rendimenti):
    s_clean = serie_rendimenti.replace([np.inf, -np.inf], np.nan).fillna(0)
    return 100 * (1 + s_clean).cumprod()


# ==============================================================================
# PARTE 1: MODELLI VAR AGGREGATI (Combinazione Lineare dei Beta)
# Aziende: MS, EXC, PSA, AEP
# ==============================================================================
print("--- Generazione Grafici VAR Aggregati ---")

config_var = [
    {'ticker': 'MS',  'indicatore': 'FINBERT',  'lag': 2},
    {'ticker': 'EXC', 'indicatore': 'FINBERT',  'lag': 1},
    {'ticker': 'PSA', 'indicatore': 'TEXTBLOB', 'lag': 3},
    {'ticker': 'AEP', 'indicatore': 'TEXTBLOB', 'lag': 3}
]

aziende_var = [c['ticker'] for c in config_var]
colonne_necessarie = [
    'Ticker', 'WeekEndingFriday', 'AdjClosePrice',
    'NEWS_TEXTBLOB_Polarity_Mean', 'NEWS_TEXTBLOB_Subjectivity_Mean',
    'NEWS_FINBERT_Negative_Mean', 'NEWS_FINBERT_Neutral_Mean', 'NEWS_FINBERT_Positive_Mean'
]

df_plot_var = df_completo[df_completo['Ticker'].isin(aziende_var)][colonne_necessarie].copy()
df_plot_var['WeekEndingFriday'] = pd.to_datetime(df_plot_var['WeekEndingFriday'])
df_plot_var = df_plot_var.sort_values(by=['Ticker', 'WeekEndingFriday'])

df_plot_var['Rendimenti'] = df_plot_var.groupby('Ticker')['AdjClosePrice'].pct_change()
df_plot_var['Rendimenti_Base100'] = df_plot_var.groupby('Ticker')['Rendimenti'].transform(calcola_base_100_azioni)

for config in config_var:
    ticker, indicatore, lag = config['ticker'], config['indicatore'], config['lag']
    
    df_az = df_plot_var[df_plot_var['Ticker'] == ticker].copy()
    riga_granger = df_granger[df_granger['Ticker'] == ticker]
    if df_az.empty or riga_granger.empty: continue

    # Calcolo Score Grezzo
    if indicatore == 'TEXTBLOB':
        b1 = riga_granger[f'Granger_beta1_lag_{lag}_TEXTBLOB'].values[0]
        b2 = riga_granger[f'Granger_beta2_lag_{lag}_TEXTBLOB'].values[0]
        if pd.notna(b1) and pd.notna(b2):
            df_az['Score_Sentiment'] = (b1 * df_az['NEWS_TEXTBLOB_Polarity_Mean'] + 
                                        b2 * df_az['NEWS_TEXTBLOB_Subjectivity_Mean'])
        else: continue

    elif indicatore == 'FINBERT':
        b1 = riga_granger[f'Granger_beta1_lag_{lag}_FINBERT'].values[0]
        b2 = riga_granger[f'Granger_beta2_lag_{lag}_FINBERT'].values[0]
        b3 = riga_granger[f'Granger_beta3_lag_{lag}_FINBERT'].values[0]
        if pd.notna(b1) and pd.notna(b2) and pd.notna(b3):
            df_az['Score_Sentiment'] = (b1 * df_az['NEWS_FINBERT_Negative_Mean'] + 
                                        b2 * df_az['NEWS_FINBERT_Neutral_Mean'] + 
                                        b3 * df_az['NEWS_FINBERT_Positive_Mean'])
        else: continue

    # Creazione LAG
    df_az['Score_Sentiment_Lag'] = df_az['Score_Sentiment'].shift(lag)
    
    # Adattamento di Scala (Evita curve piatte)
    valori_validi_sent = df_az['Score_Sentiment_Lag'].dropna()
    if not valori_validi_sent.empty:
        primo_valore = valori_validi_sent.iloc[0]
        std_sentiment = valori_validi_sent.std()
        std_azioni = df_az['Rendimenti_Base100'].dropna().std()
        if std_sentiment > 0:
            z_score = (df_az['Score_Sentiment_Lag'] - primo_valore) / std_sentiment
            df_az['Sentiment_Base100'] = 100 + (z_score * std_azioni)
        else: df_az['Sentiment_Base100'] = 100
    else: df_az['Sentiment_Base100'] = np.nan

    # Plot
    plt.figure(figsize=(14, 6))
    plt.plot(df_az['WeekEndingFriday'], df_az['Rendimenti_Base100'], label=f'Rendimenti {ticker} (Base 100)', color='royalblue', linewidth=2.5)
    plt.plot(df_az['WeekEndingFriday'], df_az['Sentiment_Base100'], label=f'Sentiment {indicatore} Aggregato (Scala Adattiva, Lag={lag})', color='darkorange', linewidth=2.5, linestyle='--')
    plt.title(f'Azienda: {ticker} | Modello VAR: {indicatore} | Lag: {lag}', fontsize=14, fontweight='bold')
    plt.xlabel('Data (WeekEndingFriday)', fontsize=12)
    plt.ylabel('Valore Indice Base 100', fontsize=12)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)
    plt.legend(loc='best', shadow=True)
    plt.grid(True, linestyle=':', alpha=0.7)
    plt.tight_layout()
    plt.show()


# ==============================================================================
# PARTE 2: SINGOLI INDICATORI
# Aziende: UL, JNJ, PG, COST, EQNR, D, MSFT, SCHW, EPD
# ==============================================================================
print("--- Generazione Grafici Singoli Indicatori ---")

config_singole = [
    {'ticker': 'UL',   'indicatore': 'NEWS_FINBERT_Negative_Mean',      'lag': 2},
    {'ticker': 'JNJ',  'indicatore': 'NEWS_FINBERT_Positive_Mean',      'lag': 3},
    {'ticker': 'PG',   'indicatore': 'NEWS_TEXTBLOB_Subjectivity_Mean', 'lag': 3},
    {'ticker': 'COST', 'indicatore': 'NEWS_FINBERT_Neutral_Mean',       'lag': 2},
    {'ticker': 'EQNR', 'indicatore': 'NEWS_TEXTBLOB_Polarity_Mean',     'lag': 3},
    {'ticker': 'D',    'indicatore': 'NEWS_FINBERT_Negative_Mean',      'lag': 4},
    {'ticker': 'MSFT', 'indicatore': 'NEWS_TEXTBLOB_Subjectivity_Mean', 'lag': 2},
    {'ticker': 'SCHW', 'indicatore': 'NEWS_TEXTBLOB_Polarity_Mean',     'lag': 3},
    {'ticker': 'EPD',  'indicatore': 'NEWS_FINBERT_Negative_Mean',      'lag': 1}
]

aziende_singole = [c['ticker'] for c in config_singole]

# Lavoriamo sul dataset df
df_plot_singole = df[df['Ticker'].isin(aziende_singole)].copy()
df_plot_singole['WeekEndingFriday'] = pd.to_datetime(df_plot_singole['WeekEndingFriday'])
df_plot_singole = df_plot_singole.sort_values(by=['Ticker', 'WeekEndingFriday'])

df_plot_singole['Rendimenti'] = df_plot_singole.groupby('Ticker')['AdjClosePrice'].pct_change()
df_plot_singole['Rendimenti_Base100'] = df_plot_singole.groupby('Ticker')['Rendimenti'].transform(calcola_base_100_azioni)

for config in config_singole:
    ticker, indicatore, lag = config['ticker'], config['indicatore'], config['lag']
    
    df_az = df_plot_singole[df_plot_singole['Ticker'] == ticker].copy()
    if df_az.empty or indicatore not in df_az.columns: continue

    # Creazione LAG sul singolo indicatore
    df_az['Indicatore_Lag'] = df_az[indicatore].shift(lag)
    
    # Adattamento di Scala (Evita curve piatte)
    valori_validi_ind = df_az['Indicatore_Lag'].dropna()
    if not valori_validi_ind.empty:
        primo_valore = valori_validi_ind.iloc[0]
        std_ind = valori_validi_ind.std()
        std_azioni = df_az['Rendimenti_Base100'].dropna().std()
        if std_ind > 0:
            z_score = (df_az['Indicatore_Lag'] - primo_valore) / std_ind
            df_az['Indicatore_Base100'] = 100 + (z_score * std_azioni)
        else: df_az['Indicatore_Base100'] = 100
    else: df_az['Indicatore_Base100'] = np.nan

    # Plot
    plt.figure(figsize=(14, 6))
    plt.plot(df_az['WeekEndingFriday'], df_az['Rendimenti_Base100'], label=f'Rendimenti {ticker} (Base 100)', color='royalblue', linewidth=2.5)
    plt.plot(df_az['WeekEndingFriday'], df_az['Indicatore_Base100'], label=f'{indicatore} (Base 100, Lag={lag})', color='crimson', linewidth=2.5, linestyle='--')
    plt.title(f'Indicatore: {indicatore} | Lag: {lag} | Azienda: {ticker}', fontsize=14, fontweight='bold')
    plt.xlabel('Data (WeekEndingFriday)', fontsize=12)
    plt.ylabel('Valore Indice Base 100', fontsize=12)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)
    plt.legend(loc='best', shadow=True)
    plt.grid(True, linestyle=':', alpha=0.7)
    plt.tight_layout()
    plt.show()