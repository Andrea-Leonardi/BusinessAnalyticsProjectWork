#%%
import pandas as pd
import numpy as np 
import sys 
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg 


bag_of_words_df = pd.read_csv(cfg.VECTORIZATION_BAG_OF_WORDS_ARTICLES)
tf_idf_df = pd.read_csv(cfg.VECTORIZATION_TFIDF_ARTICLES)

financial_phrasebank_bag_of_words_df = pd.read_csv(cfg.VECTORIZATION_BAG_OF_WORDS_FINANCIAL_PHRASEBANK)
financial_phrasebank_tf_idf_df = pd.read_csv(cfg.VECTORIZATION_TFIDF_FINANCIAL_PHRASEBANK)
"""
da eliminare serve solo per una prova, per vedere se il codice funziona
"""
bag_of_words_df = bag_of_words_df.iloc[0:10, :]
tf_idf_df = tf_idf_df.iloc[0:10, :]