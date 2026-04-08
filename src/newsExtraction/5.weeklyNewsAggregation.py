"""
5.weeklyNewsAggregation.py

Scopo del file:
- prendere l'output articolo-per-articolo di textAnalysis.csv
- trasformarlo in feature settimanali per Ticker e WeekEndingFriday
- allineare queste feature al calendario settimanale gia presente in fullData.csv
- salvare sia il file news aggregato sia una versione di fullData gia arricchita

Idea generale del flusso:
1. legge textAnalysis.csv e fullData.csv
2. converte ogni data articolo nel suo WeekEndingFriday
3. calcola la media settimanale delle metriche di sentiment per Ticker
4. riallinea il risultato alle chiavi Ticker + WeekEndingFriday di fullData
5. salva il file aggregato e il merge finale senza toccare fullData.csv originale
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg


# ---------------------------------------------------------------------------
# PARAMETRI E COLONNE CHIAVE
# ---------------------------------------------------------------------------

# Queste sono le chiavi che permettono di unire le feature news al dataset
# settimanale finale senza ambiguita.
KEY_COLUMNS = ["WeekEndingFriday", "Ticker"]

# Le prime tre colonne di textAnalysis identificano l'articolo.
# Tutto il resto viene trattato come metrica numerica da aggregare.
ARTICLE_ID_COLUMNS = ["ID", "Ticker", "Date"]
NON_METRIC_COLUMNS = set(ARTICLE_ID_COLUMNS + ["WeekEndingFriday"])

# Prefisso e suffisso chiari per distinguere le feature aggregate news
# dal resto delle colonne gia presenti in fullData.csv.
NEWS_FEATURE_PREFIX = "NEWS_"
NEWS_FEATURE_SUFFIX = "_Mean"


# ---------------------------------------------------------------------------
# FUNZIONI DI SUPPORTO SULLE DATE
# ---------------------------------------------------------------------------

def to_week_ending_friday(date_series: pd.Series) -> pd.Series:
    # Converte una data articolo nel venerdi di chiusura della sua settimana.
    parsed_dates = pd.to_datetime(date_series, errors="coerce", utc=True)
    parsed_dates = parsed_dates.dt.tz_convert(None)
    return (
        parsed_dates.dt.to_period("W-FRI").dt.to_timestamp(how="end").dt.normalize()
    )


def normalize_full_data_dates(date_series: pd.Series) -> pd.Series:
    # Normalizza la colonna WeekEndingFriday del fullData per avere lo stesso
    # formato temporale usato dalle feature news aggregate.
    return pd.to_datetime(date_series, errors="coerce").dt.normalize()


# ---------------------------------------------------------------------------
# FUNZIONI DI SUPPORTO SULLE FEATURE
# ---------------------------------------------------------------------------

def get_metric_columns(text_analysis_df: pd.DataFrame) -> list[str]:
    # Considero metriche tutte le colonne diverse dalle chiavi articolo.
    return [column for column in text_analysis_df.columns if column not in NON_METRIC_COLUMNS]


def build_weekly_news_features(text_analysis_df: pd.DataFrame) -> pd.DataFrame:
    # 1. Copio il dataframe per non modificare quello letto da CSV.
    working_df = text_analysis_df.copy()

    # 2. Porto ogni articolo nel calendario settimanale usato dal progetto.
    working_df["WeekEndingFriday"] = to_week_ending_friday(working_df["Date"])

    # 3. Elimino le righe senza ticker o senza data interpretabile.
    working_df["Ticker"] = working_df["Ticker"].astype(str).str.strip()
    working_df = working_df.dropna(subset=["WeekEndingFriday"])
    working_df = working_df[working_df["Ticker"].ne("")]

    # 4. Individuo le colonne metriche e le forzo a numerico.
    metric_columns = get_metric_columns(working_df)
    working_df[metric_columns] = working_df[metric_columns].apply(
        pd.to_numeric, errors="coerce"
    )

    # 5. Calcolo la media settimanale per azienda su tutte le metriche disponibili.
    weekly_metrics = (
        working_df.groupby(KEY_COLUMNS, as_index=False)[metric_columns].mean()
    )

    # 6. Aggiungo un conteggio articoli utile per controllare quanto e denso
    # il segnale news in ciascuna settimana.
    article_counts = (
        working_df.groupby(KEY_COLUMNS, as_index=False)
        .size()
        .rename(columns={"size": "NEWS_ArticleCount"})
    )

    weekly_metrics = weekly_metrics.merge(article_counts, on=KEY_COLUMNS, how="left")

    # 7. Rinomino le metriche aggregate con un prefisso esplicito.
    rename_map = {
        column: f"{NEWS_FEATURE_PREFIX}{column}{NEWS_FEATURE_SUFFIX}"
        for column in metric_columns
    }
    weekly_metrics = weekly_metrics.rename(columns=rename_map)

    return weekly_metrics.sort_values(KEY_COLUMNS).reset_index(drop=True)


# ---------------------------------------------------------------------------
# FLUSSO PRINCIPALE
# ---------------------------------------------------------------------------

def main():
    # -----------------------------------------------------------------------
    # 1. LETTURA DEI FILE DI INPUT
    # -----------------------------------------------------------------------

    text_analysis_df = pd.read_csv(cfg.ANALYSIS_TEXT)
    full_data_df = pd.read_csv(cfg.FULL_DATA)

    # -----------------------------------------------------------------------
    # 2. COSTRUZIONE DELLE FEATURE NEWS SETTIMANALI
    # -----------------------------------------------------------------------

    weekly_news_df = build_weekly_news_features(text_analysis_df)

    # -----------------------------------------------------------------------
    # 3. ALLINEAMENTO ALLE CHIAVI REALI DI FULLDATA
    # -----------------------------------------------------------------------

    # Uso direttamente le chiavi presenti in fullData per ottenere un file
    # aggregato perfettamente compatibile con il dataset finale.
    full_keys_df = full_data_df[KEY_COLUMNS].copy()
    full_keys_df["WeekEndingFriday"] = normalize_full_data_dates(
        full_keys_df["WeekEndingFriday"]
    )
    full_keys_df["Ticker"] = full_keys_df["Ticker"].astype(str).str.strip()
    full_keys_df = full_keys_df.drop_duplicates().sort_values(KEY_COLUMNS)

    aligned_weekly_news_df = full_keys_df.merge(
        weekly_news_df,
        on=KEY_COLUMNS,
        how="left",
    )

    # -----------------------------------------------------------------------
    # 4. MERGE CON IL DATASET FINALE COMPLETO
    # -----------------------------------------------------------------------

    full_data_for_merge = full_data_df.copy()
    full_data_for_merge["WeekEndingFriday"] = normalize_full_data_dates(
        full_data_for_merge["WeekEndingFriday"]
    )
    full_data_for_merge["Ticker"] = full_data_for_merge["Ticker"].astype(str).str.strip()

    full_data_with_news_df = full_data_for_merge.merge(
        aligned_weekly_news_df,
        on=KEY_COLUMNS,
        how="left",
    )

    # -----------------------------------------------------------------------
    # 5. SALVATAGGIO DEI FILE DI OUTPUT
    # -----------------------------------------------------------------------

    # Mantengo un file intermedio solo-news gia allineato a fullData e un file
    # finale completo con le colonne news gia aggiunte.
    aligned_weekly_news_to_save = aligned_weekly_news_df.copy()
    full_data_with_news_to_save = full_data_with_news_df.copy()

    aligned_weekly_news_to_save["WeekEndingFriday"] = aligned_weekly_news_to_save[
        "WeekEndingFriday"
    ].dt.strftime("%Y-%m-%d")
    full_data_with_news_to_save["WeekEndingFriday"] = full_data_with_news_to_save[
        "WeekEndingFriday"
    ].dt.strftime("%Y-%m-%d")

    aligned_weekly_news_to_save.to_csv(
        cfg.ANALYSIS_TEXT_WEEKLY,
        index=False,
        encoding="utf-8-sig",
    )
    full_data_with_news_to_save.to_csv(
        cfg.FULL_DATA_WITH_NEWS,
        index=False,
        encoding="utf-8-sig",
    )

    # -----------------------------------------------------------------------
    # 6. REPORT FINALE
    # -----------------------------------------------------------------------

    news_feature_columns = [
        column
        for column in aligned_weekly_news_to_save.columns
        if column not in KEY_COLUMNS
    ]
    covered_rows = aligned_weekly_news_to_save["NEWS_ArticleCount"].fillna(0).gt(0).sum()

    print(
        "Aggregazione news settimanale completata:",
        {
            "article_rows": len(text_analysis_df),
            "weekly_news_rows": len(aligned_weekly_news_to_save),
            "full_data_rows": len(full_data_with_news_to_save),
            "news_feature_columns": len(news_feature_columns),
            "rows_with_news": int(covered_rows),
            "weekly_output": str(cfg.ANALYSIS_TEXT_WEEKLY),
            "merged_output": str(cfg.FULL_DATA_WITH_NEWS),
        },
    )


if __name__ == "__main__":
    main()
