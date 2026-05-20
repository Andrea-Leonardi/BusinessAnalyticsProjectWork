#%%
import pandas as pd
import sys 
import numpy as np
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import config as cfg 
from datetime import date, timedelta

df = pd.read_csv(cfg.ALL_PRICE_DATA).sort_values(by=['Ticker', 'WeekEndingFriday'])[['Ticker', 'WeekEndingFriday', 'AdjClosePrice', 'AdjClosePrice_t+1']]
up = pd.read_csv("data-up.csv").sort_values(by=['Ticker', 'date'])[['Ticker', 'date', 'predicted_value', 'actual_value']]
down = pd.read_csv("data-down.csv").sort_values(by=['Ticker', 'date'])[['Ticker', 'date', 'predicted_value', 'actual_value']]



df['WeekEndingFriday'] = pd.to_datetime(df['WeekEndingFriday']).dt.date
up['date'] = pd.to_datetime(up['date']).dt.date
down['date'] = pd.to_datetime(down['date']).dt.date

uniti = pd.concat([up, down], ignore_index=True)
rendimenti = pd.DataFrame()
# %%
def calcola_rendimento(x):
    azienda = df[(df['Ticker'] == x['Ticker']) & (df['WeekEndingFriday'] == x['date'])]
    
    if not azienda.empty:
        prezzo_futuro = azienda['AdjClosePrice_t+1'].iloc[0]
        prezzo_attuale = azienda['AdjClosePrice'].iloc[0]
        rendimento = (prezzo_futuro - prezzo_attuale) / prezzo_attuale
        
        if x['predicted_value'] == 'Down':
            return -rendimento
        else:
            return rendimento
    else:
        return 0.0

rendimenti['rendimento_percentuale'] = uniti.apply(calcola_rendimento, axis=1)

# 2. Calcola la MEDIA dei rendimenti (non la somma!)
rendimento_medio = rendimenti['rendimento_percentuale'].mean()
print(f"Rendimento medio per operazione: {rendimento_medio:.2%}")

# Se proprio vuoi vedere la somma algebrica bruta (solo a scopo di punteggio del modello):
print(f"Somma algebrica dei rendimenti: {rendimenti['rendimento_percentuale'].sum():.2%}")

# %%
def calcola_rendimento_up(x):
    azienda = df[(df['Ticker'] == x['Ticker']) & (df['WeekEndingFriday'] == x['date'])]
    
    if not azienda.empty:
        prezzo_futuro = azienda['AdjClosePrice_t+1'].iloc[0]
        prezzo_attuale = azienda['AdjClosePrice'].iloc[0]
        rendimento = (prezzo_futuro - prezzo_attuale) / prezzo_attuale
        if not azienda.empty:
            prezzo_futuro = azienda['AdjClosePrice_t+1'].iloc[0]
            prezzo_attuale = azienda['AdjClosePrice'].iloc[0]
            return (prezzo_futuro - prezzo_attuale) / prezzo_attuale
        else:
            return 0.0

rendimenti['rendimento_percentuale_up'] = up.apply(calcola_rendimento_up, axis=1)

# 2. Calcola la MEDIA dei rendimenti (non la somma!)
rendimento_medio = rendimenti['rendimento_percentuale_up'].mean()
print(f"Rendimento medio per operazione: {rendimento_medio:.2%}")

# Se proprio vuoi vedere la somma algebrica bruta (solo a scopo di punteggio del modello):
print(f"Somma algebrica dei rendimenti: {rendimenti['rendimento_percentuale_up'].sum():.2%}")
# %%
def calcola_rendimento_down(x):
    azienda = df[(df['Ticker'] == x['Ticker']) & (df['WeekEndingFriday'] == x['date'])]
    
    if not azienda.empty:
        prezzo_futuro = azienda['AdjClosePrice_t+1'].iloc[0]
        prezzo_attuale = azienda['AdjClosePrice'].iloc[0]
        rendimento = (prezzo_futuro - prezzo_attuale) / prezzo_attuale
        if not azienda.empty:
            prezzo_futuro = azienda['AdjClosePrice_t+1'].iloc[0]
            prezzo_attuale = azienda['AdjClosePrice'].iloc[0]
            return -(prezzo_futuro - prezzo_attuale) / prezzo_attuale
        else:
            return 0.0

rendimenti['rendimento_percentuale_down'] = down.apply(calcola_rendimento_down, axis=1)

# 2. Calcola la MEDIA dei rendimenti (non la somma!)
rendimento_medio = rendimenti['rendimento_percentuale_down'].mean()
print(f"Rendimento medio per operazione: {rendimento_medio:.2%}")

# Se proprio vuoi vedere la somma algebrica bruta (solo a scopo di punteggio del modello):
print(f"Somma algebrica dei rendimenti: {rendimenti['rendimento_percentuale_down'].sum():.2%}")
# %%
# Facciamo un incrocio (inner join) tra df e up
df_filtrato = pd.merge(
    df, 
    up[['Ticker', 'date']],                  # Prendiamo solo Ticker e data da 'up'
    left_on=['Ticker', 'WeekEndingFriday'],  # I nomi delle colonne in 'df'
    right_on=['Ticker', 'date'],             # I nomi delle colonne in 'up'
    how='inner'                              # 'inner' tiene SOLO le righe che trovano corrispondenza
)

# Siccome il merge ti aggiungerà alla fine anche la colonna 'date' di 'up',
# puoi eliminarla per riavere esattamente la struttura originale di 'df'
df_filtrato = df_filtrato.drop(columns=['date'])

# Ora df_filtrato contiene solo le righe di df che matchano con il dataset up!
