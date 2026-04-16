"""
2.missingSummaryImputation.py

Scopo del file:
- ripulire la colonna Summary del dataset news
- individuare summary mancanti o di bassa qualita
- sostituire questi summary con il titolo della news

Idea generale del flusso:
1. legge newsArticles.csv
2. normalizza titolo e summary
3. riconosce summary scadenti con regole euristiche
4. azzera i summary scadenti
5. riempie tutti i missing finali con Headline
"""

import html
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg


# ---------------------------------------------------------------------------
# REGOLE PER RICONOSCERE SUMMARY SCADENTI
# ---------------------------------------------------------------------------

BENZINGA_BAD_TEXT_PATTERNS = [
    r"headline only article",
    r"benzinga pro traders",
    r"benzinga does not provide investment advice",
    r"all rights reserved",
    r"to add benzinga news as your preferred source on google",
    r"never miss a trade again",
]
LOW_QUALITY_FMP_SUMMARIES = {
    "benzinga",
    "bloomberg",
    "reuters",
    "upgrades",
    "downgrades",
    "the",
    "u.s.",
    "us",
}
LOW_QUALITY_FMP_PREFIXES = (
    "according to",
    "shares of",
    "the stock",
    "stocks of",
)
LOW_QUALITY_FMP_SUFFIXES = (
    "reported",
    "according to",
    "after a report emerged that",
    "following a report that",
    "following report",
    "amid reports",
    "on report",
)

MIN_SUMMARY_WORDS = 5
MIN_SUMMARY_CHARS = 25
HIGH_TITLE_SIMILARITY = 0.92


# ---------------------------------------------------------------------------
# FUNZIONI DI NORMALIZZAZIONE
# ---------------------------------------------------------------------------

def normalize_text(text):
    # Pulisce il testo senza alterarne il significato di base.
    if pd.isna(text) or not isinstance(text, str):
        return ""

    cleaned = html.unescape(text)
    cleaned = cleaned.replace("\xa0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def normalize_text_for_comparison(text):
    # Produce una normalizzazione piu aggressiva per confronti e controlli
    # di qualita, senza peggiorare il testo finale salvato nel CSV.
    cleaned = normalize_text(text)
    if not cleaned:
        return ""

    # 1. Rimuove link espliciti.
    cleaned = re.sub(r"http\S+|www\S+|https\S+", "", cleaned, flags=re.MULTILINE)

    # 2. Rimuove menzioni e marker di hashtag.
    cleaned = re.sub(r"\@\w+|\#", "", cleaned)

    # 3. Rimuove punteggiatura e numeri, lasciando solo lettere e spazi.
    cleaned = re.sub(r"[^a-zA-Z\s]", " ", cleaned)

    cleaned = cleaned.lower()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def canonical_text(text):
    # Produce una versione standardizzata utile per confronti robusti.
    return normalize_text_for_comparison(text)


# ---------------------------------------------------------------------------
# VALUTAZIONE DELLA QUALITA DEL SUMMARY
# ---------------------------------------------------------------------------

def is_low_quality_summary(summary, headline):
    # Questa funzione decide se un summary va trattato come inutilizzabile.
    cleaned_summary = normalize_text(summary)
    canonical_summary = canonical_text(cleaned_summary)
    canonical_headline = canonical_text(headline)

    if not cleaned_summary:
        return True

    summary_words = canonical_summary.split()

    # Caso 1: testo troppo corto per essere davvero informativo.
    if len(cleaned_summary) < MIN_SUMMARY_CHARS or len(summary_words) < MIN_SUMMARY_WORDS:
        return True

    # Caso 2: testo composto solo da parole troppo generiche.
    if canonical_summary in LOW_QUALITY_FMP_SUMMARIES:
        return True

    # Caso 3: boilerplate editoriale o testo promozionale.
    if any(re.search(pattern, cleaned_summary, flags=re.IGNORECASE) for pattern in BENZINGA_BAD_TEXT_PATTERNS):
        return True

    # Caso 4: frasi generiche tipiche dei feed sintetici poco utili.
    if canonical_summary.startswith(LOW_QUALITY_FMP_PREFIXES) and len(summary_words) <= 9:
        return True
    if canonical_summary.endswith(LOW_QUALITY_FMP_SUFFIXES) and len(summary_words) <= 12:
        return True

    # Caso 5: testo chiaramente troncato.
    if cleaned_summary.endswith("...") or cleaned_summary.endswith("…"):
        return True
    if cleaned_summary[-1].isalnum() and len(summary_words) >= 6 and len(summary_words[-1]) <= 4:
        return True

    # Caso 6: summary praticamente uguale al titolo.
    if canonical_headline:
        similarity = SequenceMatcher(None, canonical_summary, canonical_headline).ratio()
        if similarity >= HIGH_TITLE_SIMILARITY:
            return True

        headline_tokens = set(canonical_headline.split())
        summary_tokens = set(summary_words)
        if summary_tokens and summary_tokens.issubset(headline_tokens) and len(summary_words) <= 10:
            return True

    return False


# ---------------------------------------------------------------------------
# FLUSSO PRINCIPALE
# ---------------------------------------------------------------------------

def main():
    # 1. Carico il dataset news.
    df = pd.read_csv(cfg.NEWS_ARTICLES)

    # 2. Normalizzo i testi su cui lavoro.
    df["Headline"] = df["Headline"].apply(normalize_text)
    df["Summary"] = df["Summary"].apply(normalize_text)

    # 3. Memorizzo i summary gia vuoti prima dei controlli di qualita.
    original_missing_mask = df["Summary"].eq("")

    # 4. Individuo i summary presenti ma di bassa qualita.
    low_quality_mask = df.apply(
        lambda row: row["Summary"] != "" and is_low_quality_summary(row["Summary"], row["Headline"]),
        axis=1,
    )

    # I summary di bassa qualita vengono azzerati e trattati come missing.
    df.loc[low_quality_mask, "Summary"] = ""

    # 5. Ricalcolo i missing finali dopo la pulizia.
    final_missing_mask = df["Summary"].eq("")

    # 6. Regola finale: se il summary manca, uso il titolo.
    df.loc[final_missing_mask, "Summary"] = df.loc[final_missing_mask, "Headline"].fillna("")

    # 7. Salvo il dataset aggiornato.
    df.sort_values(by=["Ticker", "Date"], ascending=[True, True], inplace=True)
    df.to_csv(cfg.NEWS_ARTICLES, index=False, encoding="utf-8-sig")

    print(
        "Imputazione summary completata:",
        {
            "rows": len(df),
            "original_missing_summaries": int(original_missing_mask.sum()),
            "low_quality_summaries_reset": int(low_quality_mask.sum()),
            "filled_from_headline": int(final_missing_mask.sum()),
            "output": str(cfg.NEWS_ARTICLES),
        },
    )


if __name__ == "__main__":
    main()
