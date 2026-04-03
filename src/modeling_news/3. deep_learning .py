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
