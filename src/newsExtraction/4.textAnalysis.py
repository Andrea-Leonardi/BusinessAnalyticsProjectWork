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
3. crea le pipeline Transformers su GPU se disponibile, altrimenti su CPU
4. analizza i testi unici per evitare lavoro duplicato
5. ricostruisce il dataset finale delle metriche e lo salva
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
FINBERT_BATCH_SIZE = int(os.getenv("TEXT_ANALYSIS_FINBERT_BATCH_SIZE", "64"))
EMOTIONS_BATCH_SIZE = int(os.getenv("TEXT_ANALYSIS_EMOTIONS_BATCH_SIZE", "32"))
ZERO_SHOT_BATCH_SIZE = int(os.getenv("TEXT_ANALYSIS_ZERO_SHOT_BATCH_SIZE", "8"))


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

    # 1. Leggo solo le colonne necessarie per l'analisi.
    df = pd.read_csv(cfg.NEWS_ARTICLES, usecols=["ID", "Ticker", "Date", "Headline", "Summary"])
    if MAX_ROWS is not None:
        df = df.iloc[:MAX_ROWS].copy()

    # 2. Costruisco il testo finale su cui calcolare le metriche.
    df["AnalysisText"] = prepare_analysis_text(df["Summary"], df["Headline"])
    non_empty_mask = df["AnalysisText"].ne("")
    unique_texts = pd.unique(df.loc[non_empty_mask, "AnalysisText"]).tolist()

    # 3. Costruisco i modelli e analizzo i testi unici.
    pipe_finbert, pipe_emotions, pipe_zeroshot = build_pipelines()
    metrics_by_text = analyze_unique_texts(
        unique_texts=unique_texts,
        pipe_finbert=pipe_finbert,
        pipe_emotions=pipe_emotions,
        pipe_zeroshot=pipe_zeroshot,
    )

    # 4. Riporto le metriche a livello riga usando il testo associato a ogni news.
    base_columns = df[["ID", "Ticker", "Date"]].copy()
    metrics_rows = [metrics_by_text.get(text, {}) for text in df["AnalysisText"]]
    metrics_df = pd.DataFrame(metrics_rows)

    text_analysis = pd.concat([base_columns, metrics_df], axis=1)
    text_analysis.sort_values(by=["Ticker", "Date"], ascending=[True, True], inplace=True)
    text_analysis.to_csv(cfg.ANALYSIS_TEXT, index=False, encoding="utf-8-sig")

    elapsed = time.time() - start_time
    print(
        "Analisi completata:",
        {
            "rows": len(df),
            "unique_non_empty_summaries": len(unique_texts),
            "zero_shot_enabled": ENABLE_ZERO_SHOT,
            "seconds": round(elapsed, 2),
        },
    )


if __name__ == "__main__":
    main()
