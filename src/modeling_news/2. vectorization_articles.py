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

df = pd.read_csv(cfg.NEWS_ARTICLES)[["ID","Ticker","Date","Summary"]]

"""
da eliminare serve solo per una prova, per vedere se il codice funziona
"""
df = df.iloc[0:10, :]

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
df['Summary'] = df['Summary'].apply(clean_text)

# Se vuoi vedere il risultato:
print(df[['ID', 'Ticker', 'Date', 'Summary']].head())

# %%
# Vettorizzazione con TF-IDF
from sklearn.feature_extraction.text import TfidfVectorizer

# Inizializza il vettorizzatore
# max_features limita il numero di colonne (parole) per non appesantire il modello
tfidf_vectorizer = TfidfVectorizer(max_features=1000) 

# Trasforma la colonna 'Summary' in una matrice numerica
tfidf_matrix = tfidf_vectorizer.fit_transform(df['Summary'])

# Converti la matrice in un DataFrame
# Le colonne avranno i nomi delle parole (es. 'bank', 'profit', 'loss')
tf_idf_df = pd.DataFrame(
    tfidf_matrix.toarray(), 
    columns=tfidf_vectorizer.get_feature_names_out(),
    index=df.index  # Molto importante per far combaciare le righe!
)

# Seleziona le colonne anagrafiche e concatena con i vettori
tf_idf_df = pd.concat([df[['ID', 'Ticker', 'Date']], tf_idf_df], axis=1)

print("Dimensioni matrice TF-IDF:", tf_idf_df.shape)
print(tf_idf_df.head())

# Vettorizzazione con Bag of Words
from sklearn.feature_extraction.text import CountVectorizer

# Inizializza il vettorizzatore
# max_features limita il numero di colonne (parole) per non appesantire il modello
bow_vectorizer = CountVectorizer(max_features=1000)

# Trasforma la colonna 'Summary' in una matrice numerica
bow_matrix = bow_vectorizer.fit_transform(df['Summary'])

# Converti la matrice in un DataFrame
# Le colonne avranno i nomi delle parole (es. 'bank', 'profit', 'loss')
bag_of_words_df = pd.DataFrame(
    bow_matrix.toarray(), 
    columns=bow_vectorizer.get_feature_names_out(),
    index=df.index  # Molto importante per far combaciare le righe!
)

# Seleziona le colonne anagrafiche e concatena con i vettori
bag_of_words_df = pd.concat([df[['ID', 'Ticker', 'Date']], bag_of_words_df], axis=1)

print("Dimensioni matrice Bag of Words:", bag_of_words_df.shape)
print(bag_of_words_df.head())



tf_idf_df.sort_values(by=["Ticker", "Date"], ascending=[False, True], inplace=True)
tf_idf_df.to_csv(cfg.VECTORIZATION_TFIDF_ARTICLES, index=False, encoding='utf-8-sig')

bag_of_words_df.sort_values(by=["Ticker", "Date"], ascending=[False, True], inplace=True)
bag_of_words_df.to_csv(cfg.VECTORIZATION_BAG_OF_WORDS_ARTICLES, index=False, encoding='utf-8-sig')

# %%
