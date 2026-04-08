import html
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg


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


def normalize_text(text):
    if pd.isna(text):
        return ""

    cleaned = html.unescape(str(text))
    cleaned = cleaned.replace("\xa0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def canonical_text(text):
    cleaned = normalize_text(text).lower()
    cleaned = re.sub(r"[^a-z0-9\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def is_low_quality_summary(summary, headline):
    cleaned_summary = normalize_text(summary)
    canonical_summary = canonical_text(cleaned_summary)
    canonical_headline = canonical_text(headline)

    if not cleaned_summary:
        return True

    summary_words = canonical_summary.split()

    # Summary troppo corto o praticamente privo di contenuto.
    if len(cleaned_summary) < MIN_SUMMARY_CHARS or len(summary_words) < MIN_SUMMARY_WORDS:
        return True

    # Summary di una sola parola o comunque composto solo da termini molto generici.
    if canonical_summary in LOW_QUALITY_FMP_SUMMARIES:
        return True

    # Frasi note di bassa qualita o boilerplate editoriale.
    if any(re.search(pattern, cleaned_summary, flags=re.IGNORECASE) for pattern in BENZINGA_BAD_TEXT_PATTERNS):
        return True

    # Summary generico/troncato costruito come frase minimale da newswire/FMP.
    if canonical_summary.startswith(LOW_QUALITY_FMP_PREFIXES) and len(summary_words) <= 9:
        return True
    if canonical_summary.endswith(LOW_QUALITY_FMP_SUFFIXES) and len(summary_words) <= 12:
        return True

    # Finali tipicamente troncati o chiaramente incompleti.
    if cleaned_summary.endswith("...") or cleaned_summary.endswith("…"):
        return True
    if cleaned_summary[-1].isalnum() and len(summary_words) >= 6 and len(summary_words[-1]) <= 4:
        return True

    # Summary praticamente uguale al titolo.
    if canonical_headline:
        similarity = SequenceMatcher(None, canonical_summary, canonical_headline).ratio()
        if similarity >= HIGH_TITLE_SIMILARITY:
            return True

        headline_tokens = set(canonical_headline.split())
        summary_tokens = set(summary_words)
        if summary_tokens and summary_tokens.issubset(headline_tokens) and len(summary_words) <= 10:
            return True

    return False


def main():
    # Carico il dataset news gia creato.
    df = pd.read_csv(cfg.NEWS_ARTICLES)

    # Normalizzo prima i campi testuali principali per lavorare su testi puliti.
    df["Headline"] = df["Headline"].apply(normalize_text)
    df["Summary"] = df["Summary"].apply(normalize_text)

    # Considero missing sia i NaN originali sia le stringhe vuote dopo la normalizzazione.
    original_missing_mask = df["Summary"].eq("")

    # Se il summary esiste ma e di bassa qualita, lo azzero e lo tratto come mancante.
    low_quality_mask = df.apply(
        lambda row: row["Summary"] != "" and is_low_quality_summary(row["Summary"], row["Headline"]),
        axis=1,
    )
    df.loc[low_quality_mask, "Summary"] = ""

    final_missing_mask = df["Summary"].eq("")

    # Quando manca il summary, uso direttamente il titolo della news.
    df.loc[final_missing_mask, "Summary"] = df.loc[final_missing_mask, "Headline"].fillna("")

    # Ordino e salvo di nuovo il file aggiornato.
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
