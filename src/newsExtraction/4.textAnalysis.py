from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pandas as pd
from textblob import TextBlob
from transformers import pipeline

try:
    import torch
except ImportError:  # pragma: no cover - torch is expected with transformers
    torch = None

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg


ZERO_SHOT_LABELS = ["Calm", "Alert", "Sure", "Vital", "Kind", "Happy"]
ENABLE_ZERO_SHOT = os.getenv("TEXT_ANALYSIS_ENABLE_ZERO_SHOT", "1").lower() not in {
    "0",
    "false",
    "no",
}
MAX_ROWS = int(os.getenv("TEXT_ANALYSIS_MAX_ROWS", "0")) or None
FINBERT_BATCH_SIZE = int(os.getenv("TEXT_ANALYSIS_FINBERT_BATCH_SIZE", "64"))
EMOTIONS_BATCH_SIZE = int(os.getenv("TEXT_ANALYSIS_EMOTIONS_BATCH_SIZE", "32"))
ZERO_SHOT_BATCH_SIZE = int(os.getenv("TEXT_ANALYSIS_ZERO_SHOT_BATCH_SIZE", "8"))


def iter_batches(items: list[str], batch_size: int):
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def clean_summaries(summary_series: pd.Series) -> pd.Series:
    cleaned = summary_series.fillna("").astype(str).str.strip()
    return cleaned.mask(cleaned.eq("nan"), "")


def prepare_analysis_text(summary_series: pd.Series, headline_series: pd.Series) -> pd.Series:
    # Se il summary manca anche dopo gli step precedenti, uso il titolo come fallback.
    cleaned_summary = clean_summaries(summary_series)
    cleaned_headline = clean_summaries(headline_series)
    return cleaned_summary.mask(cleaned_summary.eq(""), cleaned_headline)


def get_pipeline_device() -> int:
    if torch is not None and torch.cuda.is_available():
        return 0
    return -1


def get_model_kwargs() -> dict:
    if torch is not None and torch.cuda.is_available():
        return {"torch_dtype": torch.float16}
    return {}


def build_pipelines():
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
        model_kwargs=model_kwargs,
    )
    pipe_emotions = pipeline(
        "sentiment-analysis",
        model="SamLowe/roberta-base-go_emotions",
        top_k=None,
        device=device,
        model_kwargs=model_kwargs,
    )

    pipe_zeroshot = None
    if ENABLE_ZERO_SHOT:
        pipe_zeroshot = pipeline(
            "zero-shot-classification",
            model="facebook/bart-large-mnli",
            device=device,
            model_kwargs=model_kwargs,
        )

    return pipe_finbert, pipe_emotions, pipe_zeroshot


def add_scored_output(metrics_by_text: dict[str, dict], texts: list[str], outputs, prefix: str):
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
    metrics_by_text = {text: {} for text in unique_texts}

    for text in unique_texts:
        blob = TextBlob(text)
        metrics_by_text[text]["TEXTBLOB_Polarity"] = round(blob.sentiment.polarity, 4)
        metrics_by_text[text]["TEXTBLOB_Subjectivity"] = round(blob.sentiment.subjectivity, 4)

    for batch in iter_batches(unique_texts, FINBERT_BATCH_SIZE):
        outputs = pipe_finbert(batch, truncation=True)
        add_scored_output(metrics_by_text, batch, outputs, "FINBERT")

    for batch in iter_batches(unique_texts, EMOTIONS_BATCH_SIZE):
        outputs = pipe_emotions(batch, truncation=True)
        add_scored_output(metrics_by_text, batch, outputs, "EMO")

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


def main():
    start_time = time.time()

    df = pd.read_csv(cfg.NEWS_ARTICLES, usecols=["ID", "Ticker", "Date", "Headline", "Summary"])
    if MAX_ROWS is not None:
        df = df.iloc[:MAX_ROWS].copy()

    df["AnalysisText"] = prepare_analysis_text(df["Summary"], df["Headline"])
    non_empty_mask = df["AnalysisText"].ne("")
    unique_texts = pd.unique(df.loc[non_empty_mask, "AnalysisText"]).tolist()

    pipe_finbert, pipe_emotions, pipe_zeroshot = build_pipelines()
    metrics_by_text = analyze_unique_texts(
        unique_texts=unique_texts,
        pipe_finbert=pipe_finbert,
        pipe_emotions=pipe_emotions,
        pipe_zeroshot=pipe_zeroshot,
    )

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
