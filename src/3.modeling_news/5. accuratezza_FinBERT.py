#%%
from transformers import pipeline
import pandas as pd
from pathlib import Path
import sys 
from huggingface_hub import hf_hub_download
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg
# Carica il modello FinBERT
sentiment_analysis = pipeline("sentiment-analysis", model="ProsusAI/finbert")
from sklearn.metrics import accuracy_score

# Il tuo DataFrame
df = pd.read_csv(cfg.TRAINING_ARTICLES)
# Trasformiamo la colonna in una lista e passiamola al modello
 
results = sentiment_analysis(df['sentence'].tolist())

# 1. Trasforma la lista di dizionari in un DataFrame
results_df = pd.DataFrame(results)

# 2. # Definiamo il dizionario di mappatura come hai richiesto
mapping = {
    "negative": 0,
    "neutral": 1,
    "positive": 2
}

# Creiamo la nuova colonna numerica nei risultati predetti
results_df["label_id_pred"] = results_df["label"].map(mapping)

# Assicurati che i due dataframe siano allineati (stesso ordine delle righe)
y_true = df["label_id"]       # Valori reali (già numerici)
y_pred = results_df["label_id_pred"]  # Valori predetti (appena convertiti)

# Calcolo dell'accuratezza
accuracy = accuracy_score(y_true, y_pred)

print(f"Accuratezza del modello: {accuracy:.2%}")
# %%
