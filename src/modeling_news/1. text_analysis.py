#%%
import pandas as pd
import numpy as np
from textblob import TextBlob
from transformers import pipeline
import sys 
from pathlib import Path
from transformers import BertTokenizer, BertForSequenceClassification
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 

df = pd.read_csv(cfg.NEWS_ARTICLES)[["ID","Ticker","Date","Summary"]]

"""
da eliminare serve solo per una prova, per vedere se il codice funziona
"""
df = df.iloc[0:10, :]

pipe_finbert = pipeline("sentiment-analysis", model="ProsusAI/finbert", top_k=None)
pipe_emotions = pipeline("sentiment-analysis", model="SamLowe/roberta-base-go_emotions", top_k=None)
pipe_zeroshot = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

"""
qui si testano 4 diversi modelli di intelligenza artificiale per l'analisi del sentiment e delle emozioni, e uno per la classificazione zero-shot, che permette di classificare un testo in categorie personalizzate senza bisogno di addestramento specifico
e ogni modello è testato in una diversa sezione del codice, in modo da poter confrontare i risultati e capire quale modello è più adatto per l'analisi delle news finanziarie
"""

def analyze_row(row):
    # Dati anagrafici base
    res = {"ID": row["ID"], "Ticker": row["Ticker"], "Date": row["Date"]}
    
    testo = str(row["Summary"]).strip() if pd.notnull(row["Summary"]) else ""
    
    # Se il testo è vuoto, restituiamo solo i dati base (Pandas metterà NaN nel resto)
    if not testo:
        return res

    # --- Se il testo esiste, eseguiamo le analisi ---
    
    """
    ----------------------------PRIMO MODELLO: FINBERT----------------------------    
    """
    
    # 1. FinBERT (3 colonne)
    # --- 1. FINBERT (3 colonne: Positive, Negative, Neutral) ---
    fb_out = pipe_finbert(testo)[0] # Risultato prima riga
    for item in fb_out:
        res[f"FINBERT_{item['label'].capitalize()}"] = round(item['score'], 4)
    
    """
    -----------------------------SECONDO MODELLO: GOEMOTIONS----------------------------    
    """

    # 2. GoEmotions (Prendiamo 5 emozioni d'esempio per arrivare a 16 totali)
    emo_out = pipe_emotions(testo)[0] # Risultato prima riga
    for emo in emo_out:
        res[f"EMO_{emo['label'].capitalize()}"] = round(emo['score'], 4)
    """
    -----------------------------TERZO MODELLO: TEXTBLOB----------------------------    
    """

    # 3. TextBlob (2 colonne)
    blob = TextBlob(testo)
    res["TEXTBLOB_Polarity"] = round(blob.sentiment.polarity, 4)
    res["TEXTBLOB_Subjectivity"] = round(blob.sentiment.subjectivity, 4)
    
    """
    -----------------------------QUARTO MODELLO: ZERO-SHOT CLASSIFICATION---------------------------- 
    """

    # 4. Zero-Shot GPOMS (6 colonne)
    labels = ["Calm", "Alert", "Sure", "Vital", "Kind", "Happy"]
    zs = pipe_zeroshot(testo, candidate_labels=labels)
    for label, score in zip(zs['labels'], zs['scores']):
        res[f"GPOMS_{label}"] = round(score, 4)

    return res

# Applicazione del modello
text_analysis = df.apply(analyze_row, axis=1).apply(pd.Series)

text_analysis.sort_values(by=["enterprise", "date"], ascending=[False, True], inplace=True)
text_analysis.to_csv(cfg.ANALYSIS_TEXT, index=False, encoding='utf-8-sig')

# %%
