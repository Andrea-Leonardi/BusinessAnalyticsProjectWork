"""
4.textAnalysis.py

Scopo del file:
- leggere il dataset news finale
- costruire il testo da analizzare per ogni articolo
- calcolare metriche di sentiment, emozioni e zero-shot classification
- salvare il risultato in textAnalysis.csv

Idea generale del flusso:
1. legge le news gia pulite
2. usa Summary come testo principale e Headline come fallback
3. carica una cache persistente dei testi gia analizzati
4. analizza solo i testi nuovi o non ancora coperti dalla cache
5. ricostruisce il dataset finale delle metriche e aggiorna la cache
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pandas as pd
from textblob import TextBlob

# Flag rapido per spegnere il modello zero-shot quando si vuole un run piu leggero.
USE_ZERO_SHOT_MODEL = False

# I modelli richiesti da questo script sono gia in cache.
# Forzo l'offline mode per evitare tentativi di rete durante il run.
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

from transformers import pipeline

try:
    import torch
except ImportError:  # pragma: no cover
    torch = None

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg


# ---------------------------------------------------------------------------
# PARAMETRI DELL'ANALISI
# ---------------------------------------------------------------------------

ZERO_SHOT_LABELS = ["Calm", "Alert", "Sure", "Vital", "Kind", "Happy"]
ZERO_SHOT_ENV = os.getenv("TEXT_ANALYSIS_ENABLE_ZERO_SHOT")
if ZERO_SHOT_ENV is None:
    # Se non arriva niente dall'ambiente, uso il flag rapido definito in alto.
    ENABLE_ZERO_SHOT = USE_ZERO_SHOT_MODEL
else:
    # Se invece arriva una variabile d'ambiente, quella ha la precedenza.
    ENABLE_ZERO_SHOT = ZERO_SHOT_ENV.lower() not in {"0", "false", "no"}

MAX_ROWS = int(os.getenv("TEXT_ANALYSIS_MAX_ROWS", "0")) or None

# Batch size piu aggressivi per sfruttare meglio la GPU.
FINBERT_BATCH_SIZE = int(os.getenv("TEXT_ANALYSIS_FINBERT_BATCH_SIZE", "128"))
EMOTIONS_BATCH_SIZE = int(os.getenv("TEXT_ANALYSIS_EMOTIONS_BATCH_SIZE", "64"))
ZERO_SHOT_BATCH_SIZE = int(os.getenv("TEXT_ANALYSIS_ZERO_SHOT_BATCH_SIZE", "16"))

ANALYSIS_TEXT_COLUMN = "AnalysisText"
ARTICLE_BASE_COLUMNS = ["ID", "Ticker", "Date"]
CACHE_KEY_COLUMN = ANALYSIS_TEXT_COLUMN


# ---------------------------------------------------------------------------
# FUNZIONI DI SUPPORTO SUL TESTO
# ---------------------------------------------------------------------------

def iter_batches(items: list[str], batch_size: int):
    # Divide una lista in blocchi per non mandare tutto insieme alla pipeline.
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def clean_summaries(summary_series: pd.Series) -> pd.Series:
    # Normalizza i testi eliminando NaN, spazi inutili e il testo letterale "nan".
    cleaned = summary_series.fillna("").astype(str).str.strip()
    return cleaned.mask(cleaned.eq("nan"), "")


def prepare_analysis_text(summary_series: pd.Series, headline_series: pd.Series) -> pd.Series:
    # Usa Summary come sorgente principale e Headline come fallback finale.
    cleaned_summary = clean_summaries(summary_series)
    cleaned_headline = clean_summaries(headline_series)
    return cleaned_summary.mask(cleaned_summary.eq(""), cleaned_headline)


# ---------------------------------------------------------------------------
# GESTIONE DEVICE E MODELLI
# ---------------------------------------------------------------------------

def get_pipeline_device() -> int:
    # Restituisce 0 se posso usare CUDA, altrimenti -1 per CPU.
    if torch is not None and torch.cuda.is_available():
        return 0
    return -1


def get_model_kwargs() -> dict:
    # In GPU uso float16 per velocizzare il run e ridurre memoria.
    if torch is not None and torch.cuda.is_available():
        return {"dtype": torch.float16}
    return {}


def build_pipelines():
    # Costruisce tutte le pipeline Hugging Face usate nell'analisi.
    device = get_pipeline_device()
    model_kwargs = get_model_kwargs()

    if device == 0:
        print("Text analysis: uso GPU (CUDA).")
    else:
        print("Text analysis: uso CPU.")

    pipe_finbert = pipeline(
        "sentiment-analysis",
        model="ProsusAI/finbert",
        top_k=None,
        device=device,
        local_files_only=True,
        use_safetensors=False,
        model_kwargs=model_kwargs,
    )
    pipe_emotions = pipeline(
        "sentiment-analysis",
        model="SamLowe/roberta-base-go_emotions",
        top_k=None,
        device=device,
        local_files_only=True,
        use_safetensors=False,
        model_kwargs=model_kwargs,
    )

    pipe_zeroshot = None
    if ENABLE_ZERO_SHOT:
        pipe_zeroshot = pipeline(
            "zero-shot-classification",
            model="facebook/bart-large-mnli",
            device=device,
            local_files_only=True,
            use_safetensors=False,
            model_kwargs=model_kwargs,
        )

    return pipe_finbert, pipe_emotions, pipe_zeroshot


# ---------------------------------------------------------------------------
# GESTIONE CACHE PERSISTENTE
# ---------------------------------------------------------------------------

def get_required_cache_prefixes() -> list[str]:
    # La cache e considerata valida solo se copre tutte le famiglie di metriche
    # richieste nel run corrente.
    prefixes = ["TEXTBLOB_", "FINBERT_", "EMO_"]
    if ENABLE_ZERO_SHOT:
        prefixes.append("GPOMS_")
    return prefixes


def load_metrics_cache() -> pd.DataFrame:
    # Leggo la cache se esiste gia, altrimenti parto da una struttura vuota.
    if cfg.ANALYSIS_TEXT_CACHE.exists():
        cache_df = pd.read_csv(cfg.ANALYSIS_TEXT_CACHE)
    else:
        cache_df = pd.DataFrame(columns=[CACHE_KEY_COLUMN])

    if CACHE_KEY_COLUMN not in cache_df.columns:
        cache_df[CACHE_KEY_COLUMN] = ""

    cache_df = cache_df.dropna(subset=[CACHE_KEY_COLUMN]).copy()
    cache_df[CACHE_KEY_COLUMN] = cache_df[CACHE_KEY_COLUMN].astype(str)
    cache_df = cache_df[cache_df[CACHE_KEY_COLUMN].ne("")]
    cache_df = cache_df.drop_duplicates(subset=[CACHE_KEY_COLUMN], keep="last")
    return cache_df


def split_cached_and_missing_texts(
    unique_texts: list[str],
    cache_df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    # Separiamo i testi gia coperti in modo affidabile da quelli da analizzare.
    if cache_df.empty:
        return cache_df, unique_texts

    relevant_cache_df = cache_df[cache_df[CACHE_KEY_COLUMN].isin(unique_texts)].copy()
    if relevant_cache_df.empty:
        return relevant_cache_df, unique_texts

    required_prefixes = get_required_cache_prefixes()
    usable_mask = pd.Series(True, index=relevant_cache_df.index)

    for prefix in required_prefixes:
        prefix_columns = [column for column in relevant_cache_df.columns if column.startswith(prefix)]
        if not prefix_columns:
            usable_mask &= False
            continue

        # Se una famiglia di metriche ha almeno un valore valido, considero
        # quel blocco presente. In questa pipeline i modelli scrivono l'intero
        # gruppo di output insieme, quindi la regola e sufficiente.
        usable_mask &= relevant_cache_df[prefix_columns].notna().any(axis=1)

    reusable_cache_df = relevant_cache_df.loc[usable_mask].copy()
    cached_texts = set(reusable_cache_df[CACHE_KEY_COLUMN].tolist())
    missing_texts = [text for text in unique_texts if text not in cached_texts]

    return reusable_cache_df, missing_texts


def metrics_dict_to_df(metrics_by_text: dict[str, dict]) -> pd.DataFrame:
    # Trasforma il dizionario delle metriche in un dataframe adatto alla cache.
    if not metrics_by_text:
        return pd.DataFrame(columns=[CACHE_KEY_COLUMN])

    rows = []
    for text, metrics in metrics_by_text.items():
        row = {CACHE_KEY_COLUMN: text}
        row.update(metrics)
        rows.append(row)

    return pd.DataFrame(rows)


def update_metrics_cache(existing_cache_df: pd.DataFrame, new_metrics_df: pd.DataFrame) -> pd.DataFrame:
    # Aggiungo alla cache solo i risultati nuovi e tengo una sola riga per testo.
    if new_metrics_df.empty:
        return existing_cache_df

    updated_cache_df = pd.concat([existing_cache_df, new_metrics_df], ignore_index=True, sort=False)
    updated_cache_df = updated_cache_df.drop_duplicates(subset=[CACHE_KEY_COLUMN], keep="last")
    updated_cache_df = updated_cache_df.sort_values(CACHE_KEY_COLUMN).reset_index(drop=True)
    return updated_cache_df


# ---------------------------------------------------------------------------
# RACCOLTA DELLE METRICHE
# ---------------------------------------------------------------------------

def add_scored_output(metrics_by_text: dict[str, dict], texts: list[str], outputs, prefix: str):
    # Inserisce nel dizionario finale gli score generati da una pipeline.
    for text, scored_items in zip(texts, outputs):
        for item in scored_items:
            label = item["label"].replace(" ", "_").capitalize()
            metrics_by_text[text][f"{prefix}_{label}"] = round(item["score"], 4)


def analyze_unique_texts(
    unique_texts: list[str],
    pipe_finbert,
    pipe_emotions,
    pipe_zeroshot,
) -> dict[str, dict]:
    # Analizza una sola volta ogni testo unico per ridurre i tempi di esecuzione.
    metrics_by_text = {text: {} for text in unique_texts}

    # -----------------------------------------------------------------------
    # 1. TEXTBLOB
    # -----------------------------------------------------------------------

    for text in unique_texts:
        blob = TextBlob(text)
        metrics_by_text[text]["TEXTBLOB_Polarity"] = round(blob.sentiment.polarity, 4)
        metrics_by_text[text]["TEXTBLOB_Subjectivity"] = round(blob.sentiment.subjectivity, 4)

    # -----------------------------------------------------------------------
    # 2. FINBERT
    # -----------------------------------------------------------------------

    for batch in iter_batches(unique_texts, FINBERT_BATCH_SIZE):
        outputs = pipe_finbert(batch, truncation=True)
        add_scored_output(metrics_by_text, batch, outputs, "FINBERT")

    # -----------------------------------------------------------------------
    # 3. GOEMOTIONS
    # -----------------------------------------------------------------------

    for batch in iter_batches(unique_texts, EMOTIONS_BATCH_SIZE):
        outputs = pipe_emotions(batch, truncation=True)
        add_scored_output(metrics_by_text, batch, outputs, "EMO")

    # -----------------------------------------------------------------------
    # 4. ZERO-SHOT
    # -----------------------------------------------------------------------

    if pipe_zeroshot is not None:
        for batch in iter_batches(unique_texts, ZERO_SHOT_BATCH_SIZE):
            outputs = pipe_zeroshot(
                batch,
                candidate_labels=ZERO_SHOT_LABELS,
                truncation=True,
            )
            if isinstance(outputs, dict):
                outputs = [outputs]

            for text, output in zip(batch, outputs):
                for label, score in zip(output["labels"], output["scores"]):
                    metrics_by_text[text][f"GPOMS_{label}"] = round(score, 4)

    return metrics_by_text


# ---------------------------------------------------------------------------
# FLUSSO PRINCIPALE
# ---------------------------------------------------------------------------

def main():
    start_time = time.time()

    # -----------------------------------------------------------------------
    # 1. LETTURA NEWS E COSTRUZIONE DEL TESTO DA ANALIZZARE
    # -----------------------------------------------------------------------

    df = pd.read_csv(cfg.NEWS_ARTICLES, usecols=["ID", "Ticker", "Date", "Headline", "Summary"])
    if MAX_ROWS is not None:
        df = df.iloc[:MAX_ROWS].copy()

    df[ANALYSIS_TEXT_COLUMN] = prepare_analysis_text(df["Summary"], df["Headline"])
    non_empty_mask = df[ANALYSIS_TEXT_COLUMN].ne("")
    unique_texts = pd.unique(df.loc[non_empty_mask, ANALYSIS_TEXT_COLUMN]).tolist()

    # -----------------------------------------------------------------------
    # 2. LETTURA CACHE E INDIVIDUAZIONE DEI TESTI DAVVERO NUOVI
    # -----------------------------------------------------------------------

    cache_df = load_metrics_cache()
    reusable_cache_df, missing_texts = split_cached_and_missing_texts(unique_texts, cache_df)

    print(
        "Cache text analysis:",
        {
            "cache_rows": len(cache_df),
            "reused_texts": len(reusable_cache_df),
            "texts_to_analyze": len(missing_texts),
        },
    )

    # -----------------------------------------------------------------------
    # 3. COSTRUZIONE MODELLI E ANALISI DEI SOLI TESTI NON IN CACHE
    # -----------------------------------------------------------------------

    new_metrics_df = pd.DataFrame(columns=[CACHE_KEY_COLUMN])
    if missing_texts:
        pipe_finbert, pipe_emotions, pipe_zeroshot = build_pipelines()
        new_metrics_by_text = analyze_unique_texts(
            unique_texts=missing_texts,
            pipe_finbert=pipe_finbert,
            pipe_emotions=pipe_emotions,
            pipe_zeroshot=pipe_zeroshot,
        )
        new_metrics_df = metrics_dict_to_df(new_metrics_by_text)

    # -----------------------------------------------------------------------
    # 4. AGGIORNAMENTO CACHE PERSISTENTE
    # -----------------------------------------------------------------------

    updated_cache_df = update_metrics_cache(cache_df, new_metrics_df)
    cfg.NEWS_EXTRACTION.mkdir(parents=True, exist_ok=True)
    updated_cache_df.to_csv(cfg.ANALYSIS_TEXT_CACHE, index=False, encoding="utf-8-sig")

    # -----------------------------------------------------------------------
    # 5. COSTRUZIONE DELL'OUTPUT ARTICOLO-PER-ARTICOLO
    # -----------------------------------------------------------------------

    available_metrics_df = updated_cache_df[
        updated_cache_df[CACHE_KEY_COLUMN].isin(unique_texts)
    ].copy()
    available_metrics_df = available_metrics_df.drop_duplicates(subset=[CACHE_KEY_COLUMN], keep="last")

    text_analysis = df.merge(
        available_metrics_df,
        on=CACHE_KEY_COLUMN,
        how="left",
    )
    text_analysis = text_analysis[ARTICLE_BASE_COLUMNS + [
        column for column in text_analysis.columns if column not in ARTICLE_BASE_COLUMNS + ["Headline", "Summary", ANALYSIS_TEXT_COLUMN]
    ]]
    text_analysis.sort_values(by=["Ticker", "Date"], ascending=[True, True], inplace=True)
    text_analysis.to_csv(cfg.ANALYSIS_TEXT, index=False, encoding="utf-8-sig")

    elapsed = time.time() - start_time
    print(
        "Analisi completata:",
        {
            "rows": len(df),
            "unique_non_empty_summaries": len(unique_texts),
            "cache_reused": len(reusable_cache_df),
            "newly_analyzed": len(missing_texts),
            "zero_shot_enabled": ENABLE_ZERO_SHOT,
            "seconds": round(elapsed, 2),
            "cache_output": str(cfg.ANALYSIS_TEXT_CACHE),
            "analysis_output": str(cfg.ANALYSIS_TEXT),
        },
    )


if __name__ == "__main__":
    main()
