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
tot_ticker = df["Ticker"].dropna().astype(str).str.strip().replace("", pd.NA).dropna().nunique()

# Ricavo il range date reale dal dataset invece di stamparne uno fisso.
parsed_dates = pd.to_datetime(df["Date"], errors="coerce", utc=True)
min_date = parsed_dates.min()
max_date = parsed_dates.max()

if pd.notna(min_date) and pd.notna(max_date):
    min_date_str = min_date.strftime("%Y-%m-%d")
    max_date_str = max_date.strftime("%Y-%m-%d")
else:
    min_date_str = "data minima non disponibile"
    max_date_str = "data massima non disponibile"

print(
    f"In totale abbiamo {tot_articoli} articoli su {tot_ticker} ticker "
    f"tra {min_date_str} e {max_date_str}."
)
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
plt.xlabel(f"Aziende ({tot_ticker} Ticker)", fontsize=12)
plt.ylabel("Numero di Articoli", fontsize=12)

# I ticker sono molti: nascondo le etichette per rendere il grafico leggibile.
plt.xticks([])

# La griglia orizzontale aiuta a confrontare meglio le altezze delle barre.
plt.grid(axis="y", linestyle="--", alpha=0.7)

plt.tight_layout()
plt.show()


# ---------------------------------------------------------------------------
# ANALISI MISSING SU FULLDATA_WITH_NEWS
# ---------------------------------------------------------------------------

# In questa sezione controllo, per ogni ticker del dataset finale arricchito
# con le news, quante righe hanno almeno un valore mancante in qualsiasi colonna.
full_data_with_news_df = pd.read_csv(cfg.FULL_DATA_WITH_NEWS)

# Una riga viene considerata problematica se contiene almeno un missing.
row_has_any_missing = full_data_with_news_df.isna().any(axis=1)
full_data_with_news_df["RowHasAnyMissing"] = row_has_any_missing

# Raggruppo per ticker e calcolo:
# - quante righe totali ha il ticker
# - quante di queste hanno almeno un missing
# - la percentuale di righe incomplete sul totale del ticker
missing_rows_by_ticker = (
    full_data_with_news_df.groupby("Ticker", as_index=False)
    .agg(
        TotalRows=("Ticker", "size"),
        RowsWithAnyMissing=("RowHasAnyMissing", "sum"),
    )
)
missing_rows_by_ticker["PctRowsWithAnyMissing"] = (
    missing_rows_by_ticker["RowsWithAnyMissing"]
    .divide(missing_rows_by_ticker["TotalRows"])
    .mul(100)
    .round(2)
)
missing_rows_by_ticker = missing_rows_by_ticker.sort_values(
    by=["RowsWithAnyMissing", "PctRowsWithAnyMissing", "Ticker"],
    ascending=[False, False, True],
)

print("\nRighe con almeno un missing per ticker in fulldata_with_news.csv:")
print(f"{'TICKER':<15} | {'RIGHE_MISSING':<15} | {'RIGHE_TOTALI':<15} | {'PCT_MISSING':<12}")
print("-" * 70)

for _, row in missing_rows_by_ticker.iterrows():
    print(
        f"{row['Ticker']:<15} | "
        f"{int(row['RowsWithAnyMissing']):<15} | "
        f"{int(row['TotalRows']):<15} | "
        f"{row['PctRowsWithAnyMissing']:<12.2f}"
    )


# ---------------------------------------------------------------------------
# ANALISI DISTINTA TRA MISSING NEWS E MISSING NON-NEWS
# ---------------------------------------------------------------------------

# Distinguo le colonne derivate dalle news dal resto del dataset, cosi posso
# capire se i missing arrivano davvero dal merge news oppure da altre feature.
news_columns = [column for column in full_data_with_news_df.columns if column.startswith("NEWS_")]
non_news_columns = [
    column
    for column in full_data_with_news_df.columns
    if column not in news_columns + ["RowHasAnyMissing"]
]

# Calcolo tre indicatori riga per riga:
# - almeno un missing nelle sole colonne news
# - tutte le colonne news mancanti
# - almeno un missing nelle colonne non-news
full_data_with_news_df["RowHasAnyNewsMissing"] = full_data_with_news_df[news_columns].isna().any(axis=1)
full_data_with_news_df["RowHasAllNewsMissing"] = full_data_with_news_df[news_columns].isna().all(axis=1)
full_data_with_news_df["RowHasAnyNonNewsMissing"] = full_data_with_news_df[non_news_columns].isna().any(axis=1)

missing_source_by_ticker = (
    full_data_with_news_df.groupby("Ticker", as_index=False)
    .agg(
        TotalRows=("Ticker", "size"),
        RowsWithAnyNewsMissing=("RowHasAnyNewsMissing", "sum"),
        RowsWithAllNewsMissing=("RowHasAllNewsMissing", "sum"),
        RowsWithAnyNonNewsMissing=("RowHasAnyNonNewsMissing", "sum"),
    )
)

missing_source_by_ticker["PctAnyNewsMissing"] = (
    missing_source_by_ticker["RowsWithAnyNewsMissing"]
    .divide(missing_source_by_ticker["TotalRows"])
    .mul(100)
    .round(2)
)
missing_source_by_ticker["PctAllNewsMissing"] = (
    missing_source_by_ticker["RowsWithAllNewsMissing"]
    .divide(missing_source_by_ticker["TotalRows"])
    .mul(100)
    .round(2)
)
missing_source_by_ticker["PctAnyNonNewsMissing"] = (
    missing_source_by_ticker["RowsWithAnyNonNewsMissing"]
    .divide(missing_source_by_ticker["TotalRows"])
    .mul(100)
    .round(2)
)

missing_source_by_ticker = missing_source_by_ticker.sort_values(
    by=["RowsWithAllNewsMissing", "RowsWithAnyNewsMissing", "Ticker"],
    ascending=[False, False, True],
)

print("\nDettaglio missing per ticker in fulldata_with_news.csv:")
print(
    f"{'TICKER':<15} | {'ALL_NEWS_MISS':<14} | {'ANY_NEWS_MISS':<14} | "
    f"{'ANY_NONNEWS_MISS':<17} | {'PCT_ALL_NEWS':<13} | {'PCT_ANY_NEWS':<13} | {'PCT_ANY_NONNEWS':<16}"
)
print("-" * 120)

for _, row in missing_source_by_ticker.iterrows():
    print(
        f"{row['Ticker']:<15} | "
        f"{int(row['RowsWithAllNewsMissing']):<14} | "
        f"{int(row['RowsWithAnyNewsMissing']):<14} | "
        f"{int(row['RowsWithAnyNonNewsMissing']):<17} | "
        f"{row['PctAllNewsMissing']:<13.2f} | "
        f"{row['PctAnyNewsMissing']:<13.2f} | "
        f"{row['PctAnyNonNewsMissing']:<16.2f}"
    )
