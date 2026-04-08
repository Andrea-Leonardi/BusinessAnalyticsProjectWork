"""
newsDataAnalysis.py

Scopo del file:
- fare una lettura descrittiva veloce del dataset news finale
- contare i valori nulli delle colonne principali
- mostrare quanti articoli ha ogni ticker
- visualizzare un grafico con la frequenza articoli per azienda

Questo file non modifica i dati.
Serve solo per controlli descrittivi e ispezione visiva del dataset.
"""

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg


# ---------------------------------------------------------------------------
# LETTURA DEL DATASET
# ---------------------------------------------------------------------------

df = pd.read_csv(cfg.NEWS_ARTICLES)


# ---------------------------------------------------------------------------
# ANALISI DEI VALORI NULLI
# ---------------------------------------------------------------------------

def conta_valori_nulli(colonna):
    # Conta quanti valori nulli ha una singola colonna.
    null_counts = colonna.isnull().sum()
    return null_counts


null = df.apply(conta_valori_nulli, axis=0)
tot_articoli = len(df)

print(f"In totale abbiamo {tot_articoli} articoli sulle 110 tra il 2021-01-01 e il 2026-03-27.")
print(f"Nella colonna ID abbiamo {null['ID']} valori nulli.")
print(f"Nella colonna Ticker abbiamo {null['Ticker']} valori nulli.")
print(f"Nella colonna Date abbiamo {null['Date']} valori nulli.")
print(f"Nella colonna Headline abbiamo {null['Headline']} valori nulli.")
print(f"Nella colonna Summary abbiamo {null['Summary']} valori nulli.")


# ---------------------------------------------------------------------------
# CONTEGGIO ARTICOLI PER TICKER
# ---------------------------------------------------------------------------

articoli_ordinati = df.groupby("Ticker").size().sort_values(ascending=True)

print(f"{'TICKER':<15} | {'ARTICOLI':<10}")
print("-" * 30)

for ticker, conteggio in articoli_ordinati.items():
    print(f"{ticker:<15} | {conteggio:<10}")


# ---------------------------------------------------------------------------
# GRAFICO DELLA DISTRIBUZIONE ARTICOLI
# ---------------------------------------------------------------------------

plt.figure(figsize=(15, 7))
articoli_ordinati.plot(kind="bar", color="skyblue", width=0.8)

plt.title("Frequenza Articoli per Azienda (Ordine Crescente)", fontsize=15)
plt.xlabel("Aziende (110 Ticker)", fontsize=12)
plt.ylabel("Numero di Articoli", fontsize=12)

# I ticker sono molti: nascondo le etichette per rendere il grafico leggibile.
plt.xticks([])

# La griglia orizzontale aiuta a confrontare meglio le altezze delle barre.
plt.grid(axis="y", linestyle="--", alpha=0.7)

plt.tight_layout()
plt.show()
