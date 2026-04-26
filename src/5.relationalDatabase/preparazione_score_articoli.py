#%%
from pathlib import Path
import sys 
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg
from statsmodels.tsa.vector_ar.var_model import VAR

enterprise = pd.read_csv(cfg.ENT).sort_values(by='Ticker')[['Ticker', 'sector']]
score_FINBERT_df = pd.read_csv(cfg.MODELING_DATASET).sort_values(by=['Ticker', 'WeekEndingFriday'])[['Ticker', 'WeekEndingFriday', 'AdjClosePrice', 'NEWS_FINBERT_Negative_Mean', 'NEWS_FINBERT_Neutral_Mean', 'NEWS_FINBERT_Positive_Mean']]
# 1. Definisci la lista dei nomi correttamente (senza duplicati e con le doppie quadre)
nomi_colonne = [
    'Ticker',
    'Sector',
    'const_lag1', 'L1.AdjClosePrice_lag1', 'L1.NEWS_FINBERT_Negative_Mean_lag1', 
    'L1.NEWS_FINBERT_Positive_Mean_lag1', 'pvalue_lag1',
    'const_lag2',
    'L1.AdjClosePrice_lag2', 'L1.NEWS_FINBERT_Negative_Mean_lag2', 'L1.NEWS_FINBERT_Positive_Mean_lag2',
    'L2.AdjClosePrice_lag2', 'L2.NEWS_FINBERT_Negative_Mean_lag2', 'L2.NEWS_FINBERT_Positive_Mean_lag2',
    'pvalue_lag2'
]
df = pd.DataFrame(columns=nomi_colonne)
df['Ticker'] = score_FINBERT_df['Ticker'].unique()
df['Sector'] = enterprise['sector']
# %%
def score_articoli(df_diff):
    # applichiamo la casualità di Granger per ogni ticker separatamente 
    # usiamo lag di 1 settimana (t+1) per prevedere il prezzo futuro, che equivale a un usare lag 2, dato che i prezzi sono già a t+1
    # 1. FIT DEL MODELLO
    model = VAR(df_diff)
    results = model.fit(1)

    # 2. TEST DI CAUSALITÀ DI GRANGER (Multivariato)
    # Questo test verifica se le variabili di sentiment (X) "causano" il prezzo (Y)
    granger_test = results.test_causality('AdjClosePrice', 
                                        ['NEWS_FINBERT_Negative_Mean', 'NEWS_FINBERT_Positive_Mean'], 
                                        kind='f')

    pvalue_lag1 = float(granger_test.pvalue)                               # Prende solo il numero del P-value

    # 3. Estraiamo tutti i coefficienti
    all_coeffs_lag1 = results.params['AdjClosePrice'].values.flatten().tolist()
    # usiamo lag di 2 settimana (t+2) per prevedere il prezzo futuro, che equivale a un usare lag 3, dato che i prezzi sono già a t+1
    # 1. FIT DEL MODELLO
    model = VAR(df_diff)
    results = model.fit(2)

    # 2. TEST DI CAUSALITÀ DI GRANGER (Multivariato)
    # Questo test verifica se le variabili di sentiment (X) "causano" il prezzo (Y)
    granger_test = results.test_causality('AdjClosePrice', 
                                        ['NEWS_FINBERT_Negative_Mean', 'NEWS_FINBERT_Positive_Mean'], 
                                        kind='f')

    pvalue_lag2 = float(granger_test.pvalue)                               # Prende solo il numero del P-value

    # 3. Estraiamo tutti i coefficienti
    all_coeffs_lag2 = results.params['AdjClosePrice'].values.flatten().tolist() # Prende solo i numeri dei Beta

    return all_coeffs_lag1 + [pvalue_lag1] + all_coeffs_lag2 + [pvalue_lag2]

# %%
for ticker in df['Ticker'].unique():
    # 1. TRASFORMAZIONE: Rendiamo i dati stazionari (fondamentale per Granger)
    df_diff = score_FINBERT_df[score_FINBERT_df['Ticker'] == ticker][['AdjClosePrice', 'NEWS_FINBERT_Negative_Mean', 'NEWS_FINBERT_Positive_Mean']].diff().dropna()
    valori = score_articoli(df_diff)
    df.loc[df['Ticker'] == ticker, nomi_colonne[2:]] = valori

# %%
df.to_csv(cfg.GRANGER_LAG1_LAG2, index=False, encoding='utf-8-sig')

#%% 
import matplotlib.pyplot as plt
import seaborn as sns

# Configurazione estetica "Clean & Modern"
plt.rcParams['figure.facecolor'] = 'white'
sns.set_context("talk") # Rende i testi più leggibili

# Palette personalizzata (Colori eleganti e distinguibili)
# Blu polvere, Arancio bruciato, Verde bosco, Viola spento
custom_palette = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B3", "#937860"]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12), sharex=True)

# --- GRAFICO LAG 1 ---
sns.boxplot(x='Sector', y='pvalue_lag1', data=df, ax=ax1, 
            palette=custom_palette, width=0.6, fliersize=4,
            linewidth=1.5, boxprops=dict(alpha=0.8)) # alpha per un effetto più morbido
ax1.set_title('Test di Granger: Analisi P-Value Lag 1', fontsize=18, pad=20, color='#333333')
ax1.set_ylabel('P-Value', fontsize=14)
ax1.set_ylim(0, 1.05)
ax1.grid(axis='y', linestyle='--', alpha=0.3) # Griglia leggera solo orizzontale

# --- GRAFICO LAG 2 ---
sns.boxplot(x='Sector', y='pvalue_lag2', data=df, ax=ax2, 
            palette=custom_palette, width=0.6, fliersize=4,
            linewidth=1.5, boxprops=dict(alpha=0.8))
ax2.set_title('Test di Granger: Analisi P-Value Lag 2', fontsize=18, pad=20, color='#333333')
ax2.set_ylabel('P-Value', fontsize=14)
ax2.set_ylim(0, 1.05)
ax2.grid(axis='y', linestyle='--', alpha=0.3)

# Pulizia finale degli assi
sns.despine(left=True, bottom=False) # Rimuove i bordi del grafico per un look "aperto"
plt.xticks(rotation=45, ha='right', fontsize=12)
plt.xlabel('Settore Industriale', fontsize=14, labelpad=15)

plt.tight_layout()
plt.show()


# %%
