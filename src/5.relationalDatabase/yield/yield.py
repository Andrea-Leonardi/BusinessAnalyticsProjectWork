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
        return (prezzo_futuro - prezzo_attuale) / prezzo_attuale
    else:
        return 0.0  

rendimenti['rendimento_percentuale'] = uniti.apply(calcola_rendimento, axis=1)
print(f"{rendimenti['rendimento_percentuale'].sum():.2%}")









# %%
