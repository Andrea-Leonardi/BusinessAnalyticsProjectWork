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
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg

DATASET_REPO_ID = "financial_phrasebank"
DATASET_FILENAME = cfg.HF_FINANCIAL_PHRASEBANK_DATASET_FILENAME
DATASET_CONFIG = "sentences_50agree"

CONFIG_TO_FILENAME = {
    "sentences_allagree": "FinancialPhraseBank-v1.0/Sentences_AllAgree.txt",
    "sentences_75agree": "FinancialPhraseBank-v1.0/Sentences_75Agree.txt",
    "sentences_66agree": "FinancialPhraseBank-v1.0/Sentences_66Agree.txt",
    "sentences_50agree": "FinancialPhraseBank-v1.0/Sentences_50Agree.txt",
}

LABEL_TO_ID = {"negative": 0, "neutral": 1, "positive": 2}


def configure_huggingface_cache() -> None:
    cfg.HF_CACHE.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("HF_HOME", str(cfg.HF_CACHE))
    os.environ.setdefault("HUGGINGFACE_HUB_CACHE", str(cfg.HF_CACHE))
    os.environ.setdefault("HF_HUB_DISABLE_XET", "1")


def download_phrasebank_zip() -> Path:
    configure_huggingface_cache()
    local_dir = cfg.HF_FINANCIAL_PHRASEBANK_LOCAL_DIR
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
    phrasebank_df = load_phrasebank_dataframe()

    print(f"Configurazione: {DATASET_CONFIG}")
    print(f"Numero frasi: {len(phrasebank_df)}")
    print("\nDistribuzione etichette:")
    print(phrasebank_df["label"].value_counts())
    print("\nPrima riga:")
    print(phrasebank_df.iloc[0].to_dict())


if __name__ == "__main__":
    main()
    

# Chiamiamo la funzione che hai scritto nel tuo script
phrasebank_df = load_phrasebank_dataframe()

# Ora puoi vederlo!
print(phrasebank_df[['sentence', 'label', 'label_id']].head(10))

# %%
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

# %%
""" 
Avendo eseguito il codice in Colab, per non dover ripetere l'adattamento ogni volta da capo, 
cosa che ha impiegato circa 8 ore, ho salvato i risultati in un file JSON. 
Il codice qui sotto serve a caricare quei risultati e visualizzarli in modo leggibile, 
ricostruendo anche il modello ottimale trovato durante la ricerca degli iperparametri.
"""
# CARICAMENTO E VISUALIZZAZIONE RISULTATI
import json
import torch
import torch.nn as nn
from torchsummary import summary

# 1. Carica i dati dal file JSON
JSON_PATH = cfg.MODELING_NEWS_BEST_PARAMS / "migliori_parametri_rete_non_lineare.json"
with open(JSON_PATH, 'r') as f:
    dati_caricati = json.load(f)

best_p = dati_caricati['best_params']
best_val = dati_caricati['best_value']

# 2. Mappatura funzione di attivazione
act_map = {
    'ReLU': nn.ReLU(),
    'LeakyReLU': nn.LeakyReLU(),
    'ELU': nn.ELU(),
    'GELU': nn.GELU()
}
best_act = act_map[best_p['activation']]

# 3. Ricostruzione del Modello Ottimale
final_model = nn.Sequential(
    nn.Linear(2464, best_p['units_l1']), best_act,
    nn.Linear(best_p['units_l1'], best_p['units_l2']), best_act,
    nn.Linear(best_p['units_l2'], best_p['units_l3']), best_act,
    nn.Linear(best_p['units_l3'], best_p['units_l4']), best_act,
    nn.Linear(best_p['units_l4'], 3)
)

# 4. Mostra i Risultati
print(f"--- RISULTATI CARICATI ---")
print(f"Migliore Accuratezza Ottenuta: {best_val:.2%}")
print(f"Parametri Ottimi: {best_p}")

print("\n--- RIEPILOGO ARCHITETTURA ---")
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
final_model.to(device)
summary(final_model, (2464,))
# %%
