#%%
import pandas as pd
import sys 
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 

df = pd.read_csv(cfg.NEWS_ARTICLES)

# vediamo quanti valori nulli ci sono per ogni colonna

def conta_valori_nulli(colonna):
    null_counts = colonna.isnull().sum()
    return null_counts

null = df.apply(conta_valori_nulli, axis=0)

tot_articoli = len(df)

print(f"In totale abbiamo {tot_articoli} articoli sulle 110 tra il 2021-01-01 e il 2026-03-27.")
print(f"Nella colonna degli ID abbiamo {null['ID']} valori nulli.")
print(f"Nella colonna Ticker abbiamo {null['Ticker']} valori nulli.")
print(f"Nella colonna Date abbiamo {null['Date']} valori nulli.")
print(f"Nella colonna Source abbiamo {null['Source']} valori nulli.")
print(f"Nella colonna Headline abbiamo {null['Headline']} valori nulli.")
print(f"Nella colonna Summary abbiamo {null['Summary']} valori nulli.")
print(f"Nella colonna Content abbiamo {null['Content']} valori nulli.")
print(f"Nella colonna URL abbiamo {null['URL']} valori nulli.")

#%%
# vediamo qunati articoli abbiamo per ogni azienda
# 1. Crea la serie, conta e ordina dal più grande al più piccolo
articoli_ordinati = df.groupby("Ticker").size().sort_values(ascending=True)

# 2. Usa un ciclo for: è l'unico modo per bypassare i limiti di visualizzazione dei notebook
print(f"{'TICKER':<15} | {'ARTICOLI':<10}")
print("-" * 30)

for ticker, conteggio in articoli_ordinati.items():
    print(f"{ticker:<15} | {conteggio:<10}")


#%%
# python -m pip install matplotlib
import matplotlib.pyplot as plt

# 1. Prepariamo i dati ordinati (crescenti)
articoli_ordinati = df.groupby("Ticker").size().sort_values(ascending=True)

# 2. Creazione del grafico
plt.figure(figsize=(15, 7))
articoli_ordinati.plot(kind='bar', color='skyblue', width=0.8)

# 3. Personalizzazione
plt.title('Frequenza Articoli per Azienda (Ordine Crescente)', fontsize=15)
plt.xlabel('Aziende (110 Ticker)', fontsize=12)
plt.ylabel('Numero di Articoli', fontsize=12)

# Se vuoi nascondere le etichette X perché troppo affollate:
plt.xticks([]) 

# Aggiungiamo una griglia orizzontale per leggere meglio i valori
plt.grid(axis='y', linestyle='--', alpha=0.7)

plt.tight_layout()
plt.show()

# %%
