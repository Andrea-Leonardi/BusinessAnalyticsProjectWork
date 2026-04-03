#%%
import pandas as pd
from newspaper import Article
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM 
import sys 
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 
"""
appena si crea il dataste qui va inserito il percorso del csv da cui prendere i dati    

"""
df = pd.read_csv(cfg.NEWS_ARTICLES)


# QUESTO DEVE RESTARE (Caricamento del motore)
model_name = "facebook/bart-large-cnn"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)



def riassunto(testo, max_l, min_l):
    inputs = tokenizer(testo, return_tensors="pt", max_length=1024, truncation=True)
    
    summary_ids = model.generate(
        inputs["input_ids"], 
        max_length=max_l, 
        min_length=min_l, 
        forced_bos_token_id=0
    )
    return tokenizer.decode(summary_ids[0], skip_special_tokens=True)


def imputazione_articoli_mancanti(row):
    if pd.isna(row['Summary']):
       # 1. Inizializza l'articolo con l'URL preso dal DataFrame
        url = row['URL'] 
        article = Article(url)

        # 2. SCARICA l'articolo
        article.download()

        # 3. ESTRAI il contenuto (senza questo, article.text sarà None)
        article.parse()

        num_parole = len(article.text.split())
        if num_parole > 77:
            massimo = int(num_parole * 0.5)
            minimo = int(num_parole * 0.25)
            riassunnto = riassunto(article.text, max_l=massimo, min_l=minimo)
            row["Summary"] = riassunnto
            return row
        else:   
            row["Summary"] = article.text
            return row  # Se l'articolo è troppo corto, non fare nulla
        # Imposta i limiti in base alla lunghezza reale 
    return row  # Se 'Summary' non è NaN, restituisci la riga senza modifiche  
       
df = df.apply(imputazione_articoli_mancanti, axis=1)

df.sort_values(by=["enterprise", "date"], ascending=[False, True], inplace=True)
df.to_csv(cfg.NEWS_ARTICLES, index=False, encoding='utf-8-sig')

