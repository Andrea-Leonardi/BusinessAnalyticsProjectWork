#%%
"""
Carica il dataset Financial PhraseBank da Hugging Face senza usare
`datasets.load_dataset(...)`, che non supporta piu` i dataset basati su
script di caricamento come questo.
"""

from __future__ import annotations

import os
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
from huggingface_hub import hf_hub_download


PROJECT_ROOT = Path(__file__).resolve().parents[2]
HF_CACHE_DIR = PROJECT_ROOT / "data" / "hf_cache"
DATASET_REPO_ID = "financial_phrasebank"
DATASET_FILENAME = "data/FinancialPhraseBank-v1.0.zip"
DATASET_CONFIG = "sentences_50agree"

CONFIG_TO_FILENAME = {
    "sentences_allagree": "FinancialPhraseBank-v1.0/Sentences_AllAgree.txt",
    "sentences_75agree": "FinancialPhraseBank-v1.0/Sentences_75Agree.txt",
    "sentences_66agree": "FinancialPhraseBank-v1.0/Sentences_66Agree.txt",
    "sentences_50agree": "FinancialPhraseBank-v1.0/Sentences_50Agree.txt",
}

LABEL_TO_ID = {"negative": 0, "neutral": 1, "positive": 2}


def configure_huggingface_cache() -> None:
    HF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("HF_HOME", str(HF_CACHE_DIR))
    os.environ.setdefault("HUGGINGFACE_HUB_CACHE", str(HF_CACHE_DIR))
    os.environ.setdefault("HF_HUB_DISABLE_XET", "1")


def download_phrasebank_zip() -> Path:
    configure_huggingface_cache()
    local_dir = HF_CACHE_DIR / "financial_phrasebank"
    local_dir.mkdir(parents=True, exist_ok=True)

    return Path(
        hf_hub_download(
            repo_id=DATASET_REPO_ID,
            repo_type="dataset",
            filename=DATASET_FILENAME,
            local_dir=str(local_dir),
        )
    )


def load_phrasebank_dataframe(config_name: str = DATASET_CONFIG) -> pd.DataFrame:
    if config_name not in CONFIG_TO_FILENAME:
        supported = ", ".join(CONFIG_TO_FILENAME)
        raise ValueError(f"Configurazione non valida: {config_name}. Usa una di: {supported}")

    zip_path = download_phrasebank_zip()
    inner_filename = CONFIG_TO_FILENAME[config_name]

    rows: list[dict[str, str | int]] = []
    with ZipFile(zip_path) as archive:
        with archive.open(inner_filename) as text_file:
            for raw_line in text_file:
                line = raw_line.decode("latin-1").strip()
                if not line:
                    continue

                sentence, label = line.rsplit("@", 1)
                label = label.strip().lower()
                rows.append(
                    {
                        "sentence": sentence.strip(),
                        "label": label,
                        "label_id": LABEL_TO_ID[label],
                    }
                )

    return pd.DataFrame(rows)


def main() -> None:
    training_df = load_phrasebank_dataframe()

    print(f"Configurazione: {DATASET_CONFIG}")
    print(f"Numero frasi: {len(training_df)}")
    print("\nDistribuzione etichette:")
    print(training_df["label"].value_counts())
    print("\nPrima riga:")
    print(training_df.iloc[0].to_dict())


if __name__ == "__main__":
    main()
    

# Chiamiamo la funzione che hai scritto nel tuo script
training_df = load_phrasebank_dataframe()

# Ora puoi vederlo!
print(training_df[['sentence', 'label', 'label_id']].head(10))

# %%
""" 
# 1. Estrazione casuale di 4000 righe
# random_state serve a rendere l'estrazione "riproducibile" 
# (se lo riavvii, otterrai sempre le stesse 4000 righe)
df_random = phrasebank_df.sample(n=4000, random_state=42)

# 2. Reset dell'indice (opzionale ma consigliato)
# Serve per avere un nuovo indice da 0 a 3999 invece di quelli sparsi originali
df_random = df_random.reset_index(drop=True)

# Verifica il risultato
print(f"Nuovo numero di frasi: {len(df_random)}")
print(df_random.head())
"""

#%%
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import pandas as pd
import numpy as np 
import sys 
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 

"""
non tutte le stop words sono da rimuovere, ad esempio "not", "no", "nor", "n't" sono parole che indicano negazione e possono essere importanti per il significato del testo, quindi le escludiamo dalla lista delle stop words da rimuovere.
"""

# Scarica le risorse necessarie 
# nltk.download('punkt')
# nltk.download('stopwords')

#qui possiamo vedere la lista delle stop words, che sono parole comuni che non aggiungono molto significato al testo e spesso vengono rimosse durante la pulizia del testo per migliorare le prestazioni dei modelli di machine learning.

# Carica la lista delle stopwords inglesi
stop_words = stopwords.words('english')

# Ordinale alfabeticamente per leggerle meglio
stop_words.sort()
stop_words = pd.DataFrame(stop_words, columns=["Stop Words"])

print(f"Ci sono {len(stop_words)} stop words nella lista NLTK:\n")
print(stop_words)

parola_da_non_rimuovere = ["not", "no", "nor", "n't"]  # Parole che indicano negazione
posizioni = np.where(stop_words["Stop Words"].isin(parola_da_non_rimuovere))[0]
print(posizioni)

stop_words = stop_words.drop(posizioni).reset_index(drop=True)
print(f"Ci sono {len(stop_words)} stop words nella lista NLTK:\n")
print(stop_words)

# TRASFORMAZIONE: Converti la colonna del DF in un set per la funzione
stop_words = set(stop_words["Stop Words"])

# %%


stemmer = PorterStemmer() 

def clean_text(text):
    # Gestione valori nulli (se un Summary è vuoto)
    if not isinstance(text, str):
        return ""

    # 1. Rimuove Link (URL)
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    
    # 2. Rimuove menzioni (@user) e hashtag (#)
    text = re.sub(r'\@\w+|\#','', text)
    
    # 3. Rimuove punteggiatura e numeri (tiene solo le lettere)
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    
    # 4. Converte in minuscolo e Tokenizzazione
    tokens = word_tokenize(text.lower())
    
    # 5. Rimuove le Stopwords (usando il set filtrato)
    # 6. Stemming (riduce alla radice)
    # Facciamo entrambi in un unico passaggio per efficienza
    stemmed_tokens = [stemmer.stem(w) for w in tokens if w in tokens and w not in stop_words]
    
    # Ricompone i token in una stringa
    return " ".join(stemmed_tokens)

# --- APPLICAZIONE AL DATAFRAME ---

# Creiamo una nuova colonna 'Summary_Clean' mantenendo le altre colonne chiave.
training_df['sentence'] = training_df['sentence'].apply(clean_text)

# Se vuoi vedere il risultato:
# print(training_df).head())

# %%
# Vettorizzazione con TF-IDF
from sklearn.feature_extraction.text import TfidfVectorizer

# min_df=3: ignora le parole che compaiono in meno di 3 frasi
# max_df=0.85: ignora le parole che compaiono in più dell'90% delle frasi
tfidf_vectorizer = TfidfVectorizer(min_df=3, max_df=0.9)

# Trasforma la colonna 'sentence' in una matrice numerica
training_tfidf_matrix = tfidf_vectorizer.fit_transform(training_df['sentence'])

# Converti la matrice in un DataFrame
# Le colonne avranno i nomi delle parole (es. 'bank', 'profit', 'loss')
training_tf_idf_df = pd.DataFrame(
    training_tfidf_matrix.toarray(), 
    columns=tfidf_vectorizer.get_feature_names_out(),
    index=training_df.index  # Molto importante per far combaciare le righe!
)

print("Dimensioni matrice TF-IDF:", training_tf_idf_df.shape)
print(training_tf_idf_df.head())

# Vettorizzazione con Bag of Words
from sklearn.feature_extraction.text import CountVectorizer

# Inizializza il vettorizzatore
# max_features limita il numero di colonne (parole) per non appesantire il modello
bow_vectorizer = CountVectorizer(min_df=3, max_df=0.90)

# Trasforma la colonna 'sentence' in una matrice numerica
training_bow_matrix = bow_vectorizer.fit_transform(training_df['sentence'])

# Converti la matrice in un DataFrame
# Le colonne avranno i nomi delle parole (es. 'bank', 'profit', 'loss')
training_bag_of_words_df = pd.DataFrame(
    training_bow_matrix.toarray(), 
    columns=bow_vectorizer.get_feature_names_out(),
    index=training_df.index  # Molto importante per far combaciare le righe!
)


# Aggiungi label_id come prima colonna
training_tf_idf_df = pd.concat([training_df[['label_id']], training_tf_idf_df], axis=1)
training_tf_idf_df.to_csv(cfg.VECTORIZATION_TFIDF_FINANCIAL_PHRASEBANK, index=False, encoding='utf-8-sig')

training_bag_of_words_df = pd.concat([training_df[['label_id']], training_bag_of_words_df], axis=1)
training_bag_of_words_df.to_csv(cfg.VECTORIZATION_BAG_OF_WORDS_FINANCIAL_PHRASEBANK, index=False, encoding='utf-8-sig')
# %%
# --- APPLICAZIONE AL DATAFRAME ---
df = pd.read_csv(cfg.NEWS_ARTICLES)[["ID","Ticker","Date","Summary"]]

# Creiamo una nuova colonna 'Summary_Clean' mantenendo le altre colonne chiave.
df['Summary'] = df['Summary'].apply(clean_text)

# Se vuoi vedere il risultato:
print(df[['ID', 'Ticker', 'Date', 'Summary']].head())

# %%
# Vettorizzazione con TF-IDF
# -------------------------------------------------------------------------
# VETTORIZZAZIONE DELLE NEWS (INFERENCE)
# -------------------------------------------------------------------------
# IMPORTANTE: NON re-inizializzare TfidfVectorizer o CountVectorizer qui!
# Usiamo le variabili 'tfidf_vectorizer' e 'bow_vectorizer' che hai 
# addestrato sulle 4846 righe del Financial PhraseBank nei passaggi prima.

# Trasforma le news usando IL VOCABOLARIO IMPARATO DAL PHRASEBANK (le tue 2464 parole)
tfidf_matrix = tfidf_vectorizer.transform(df['Summary'])

# Converti la matrice in un DataFrame
tf_idf_df = pd.DataFrame(
    tfidf_matrix.toarray(), 
    columns=tfidf_vectorizer.get_feature_names_out(), # Saranno magicamente le 2464 parole!
    index=df.index
)

# Seleziona le colonne anagrafiche e concatena con i vettori
tf_idf_df = pd.concat([df[['ID', 'Ticker', 'Date']], tf_idf_df], axis=1)

print("Dimensioni matrice TF-IDF News:", tf_idf_df.shape)
print(tf_idf_df.head())


# --- Stessa cosa per il Bag of Words ---

# Trasforma le news usando IL VOCABOLARIO IMPARATO DAL PHRASEBANK
bow_matrix = bow_vectorizer.transform(df['Summary'])

# Converti la matrice in un DataFrame
bag_of_words_df = pd.DataFrame(
    bow_matrix.toarray(), 
    columns=bow_vectorizer.get_feature_names_out(), # Saranno le stesse 2464 parole!
    index=df.index
)

# Seleziona le colonne anagrafiche e concatena con i vettori
bag_of_words_df = pd.concat([df[['ID', 'Ticker', 'Date']], bag_of_words_df], axis=1)

print("Dimensioni matrice Bag of Words News:", bag_of_words_df.shape)
print(bag_of_words_df.head())



tf_idf_df.sort_values(by=["Ticker", "Date"], ascending=[False, True], inplace=True)
tf_idf_df.to_csv(cfg.VECTORIZATION_TFIDF_ARTICLES, index=False, encoding='utf-8-sig')

bag_of_words_df.sort_values(by=["Ticker", "Date"], ascending=[False, True], inplace=True)
bag_of_words_df.to_csv(cfg.VECTORIZATION_BAG_OF_WORDS_ARTICLES, index=False, encoding='utf-8-sig')
# %%
