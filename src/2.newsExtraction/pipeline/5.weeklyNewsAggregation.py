"""
5.weeklyNewsAggregation.py

Scopo del file:
- prendere l'output articolo-per-articolo di textAnalysis.csv
- trasformarlo in feature settimanali per Ticker e WeekEndingFriday
- allineare queste feature al calendario settimanale gia presente in fullData.csv
- salvare sia il file news aggregato sia una versione di fullData gia arricchita
- creare anche modeling.csv gia pulito dai missing per i modelli ML

Idea generale del flusso:
1. legge textAnalysis.csv e fullData.csv
2. ricostruisce anche l'Headline articolo per articolo
3. converte ogni data articolo nel suo WeekEndingFriday in ottica market-close
4. deduplica gli articoli ripetuti per Ticker + WeekEndingFriday + Headline
5. calcola la media settimanale delle metriche di sentiment per Ticker
4. riallinea il risultato alle chiavi Ticker + WeekEndingFriday di fullData
5. salva il file aggregato, il merge finale e il dataset modeling senza toccare fullData.csv originale
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import config as cfg


# ---------------------------------------------------------------------------
# PARAMETRI E COLONNE CHIAVE
# ---------------------------------------------------------------------------

# Queste sono le chiavi che permettono di unire le feature news al dataset
# settimanale finale senza ambiguita.
KEY_COLUMNS = ["WeekEndingFriday", "Ticker"]

# Le prime tre colonne di textAnalysis identificano l'articolo.
# Headline viene ricostruita da newsArticles.csv per poter fare deduplica
# a livello contenutistico prima dell'aggregazione.
ARTICLE_ID_COLUMNS = ["ID", "Ticker", "Date"]
NON_METRIC_COLUMNS = set(ARTICLE_ID_COLUMNS + ["Headline", "WeekEndingFriday"])
HEADLINE_DEDUP_COLUMNS = ["Ticker", "WeekEndingFriday", "Headline"]
ARTICLE_MATCH_GROUP_COLUMNS = ["Ticker", "Date", "ArticleIdKey"]
ARTICLE_MATCH_COLUMNS = ARTICLE_MATCH_GROUP_COLUMNS + ["ArticleDuplicateIndex"]

# Prefisso e suffisso chiari per distinguere le feature aggregate news
# dal resto delle colonne gia presenti in fullData.csv.
NEWS_FEATURE_PREFIX = "NEWS_"
NEWS_FEATURE_SUFFIX = "_Mean"
MARKET_TIMEZONE = "America/New_York"
FRIDAY_MARKET_CLOSE_HOUR = 16
FINBERT_NEGATIVE_COL = "FINBERT_Negative"
FINBERT_NEUTRAL_COL = "FINBERT_Neutral"
FINBERT_POSITIVE_COL = "FINBERT_Positive"
FINBERT_SENTIMENT_COL = "Sentiment"
GRANGER_FINBERT_COEFFICIENTS_FILE = cfg.GRANGER_FINBERT_COEFFICIENTS
GRANGER_LAG_COLUMN = "lag"
GRANGER_BETA_NEGATIVE_COL = "beta_negative"
GRANGER_BETA_NEUTRAL_COL = "beta_neutral"
GRANGER_BETA_POSITIVE_COL = "beta_positive"
GRANGER_ARTICLE_SCORE_TEMPLATE = "FINBERT_GrangerArticleScore_Lag{lag}"
GRANGER_WEEKLY_SHIFTED_SCORE_TEMPLATE = "NEWS_FINBERT_GrangerArticleScore_Lag{lag}_Shifted"
GRANGER_FINAL_SCORE_COLUMN = "NEWS_FINBERT_Granger_Score"


# ---------------------------------------------------------------------------
# FUNZIONI DI SUPPORTO SULLE DATE
# ---------------------------------------------------------------------------

def to_week_ending_friday(date_series: pd.Series) -> pd.Series:
    # Converte la data articolo nel calendario di mercato USA.
    # Gli articoli del venerdi dopo la chiusura e quelli del weekend vengono
    # spostati alla settimana successiva per evitare timing leakage.
    parsed_dates = pd.to_datetime(date_series, errors="coerce", utc=True)
    market_dates = parsed_dates.dt.tz_convert(MARKET_TIMEZONE).dt.tz_localize(None)
    week_ending = (
        market_dates.dt.to_period("W-FRI").dt.to_timestamp(how="end").dt.normalize()
    )

    after_friday_close_mask = (
        market_dates.dt.weekday.eq(4) & market_dates.dt.hour.ge(FRIDAY_MARKET_CLOSE_HOUR)
    )
    weekend_mask = market_dates.dt.weekday.ge(5)
    shift_to_next_week_mask = after_friday_close_mask | weekend_mask

    return week_ending.mask(shift_to_next_week_mask, week_ending + pd.Timedelta(days=7))


def normalize_full_data_dates(date_series: pd.Series) -> pd.Series:
    # Normalizza la colonna WeekEndingFriday del fullData per avere lo stesso
    # formato temporale usato dalle feature news aggregate.
    return pd.to_datetime(date_series, errors="coerce").dt.normalize()


def normalize_text_for_key(text_series: pd.Series) -> pd.Series:
    # Normalizzo il testo per i match e per la deduplica headline.
    cleaned = text_series.fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    return cleaned


def normalize_id_for_key(id_series: pd.Series) -> pd.Series:
    # Porto gli ID in forma testuale stabile, trattando i missing in modo coerente.
    normalized = id_series.fillna("").astype(str).str.strip()
    invalid_mask = normalized.str.lower().isin({"", "nan", "none", "<na>"})
    return normalized.mask(invalid_mask, "")


def build_article_match_frame(df: pd.DataFrame) -> pd.DataFrame:
    # Creo chiavi di matching robuste per riallineare textAnalysis con newsArticles.
    working_df = df.copy()
    working_df["Ticker"] = normalize_text_for_key(working_df["Ticker"])
    working_df["Date"] = normalize_text_for_key(working_df["Date"])
    working_df["ArticleIdKey"] = normalize_id_for_key(working_df["ID"])

    sort_columns = ARTICLE_MATCH_GROUP_COLUMNS.copy()
    if "Headline" in working_df.columns:
        working_df["Headline"] = normalize_text_for_key(working_df["Headline"])
        sort_columns.append("Headline")

    working_df = working_df.sort_values(sort_columns, kind="mergesort").copy()
    working_df["ArticleDuplicateIndex"] = (
        working_df.groupby(ARTICLE_MATCH_GROUP_COLUMNS).cumcount()
    )
    return working_df


def attach_headline_column(text_analysis_df: pd.DataFrame) -> pd.DataFrame:
    # textAnalysis.csv non contiene Headline, ma per deduplicare i boilerplate
    # settimanali serve recuperarla dal dataset news articolo-per-articolo.
    if "Headline" in text_analysis_df.columns:
        enriched_df = text_analysis_df.copy()
        enriched_df["Headline"] = normalize_text_for_key(enriched_df["Headline"])
        return enriched_df

    news_articles_df = pd.read_csv(cfg.NEWS_ARTICLES, usecols=["ID", "Ticker", "Date", "Headline"])
    text_match_df = build_article_match_frame(text_analysis_df)
    news_match_df = build_article_match_frame(news_articles_df)

    enriched_df = text_match_df.merge(
        news_match_df[ARTICLE_MATCH_COLUMNS + ["Headline"]],
        on=ARTICLE_MATCH_COLUMNS,
        how="left",
    )
    enriched_df["Headline"] = normalize_text_for_key(enriched_df["Headline"])
    return enriched_df.drop(columns=["ArticleIdKey", "ArticleDuplicateIndex"])


# ---------------------------------------------------------------------------
# FUNZIONI DI SUPPORTO SULLE FEATURE
# ---------------------------------------------------------------------------

def get_metric_columns(text_analysis_df: pd.DataFrame) -> list[str]:
    # Considero metriche tutte le colonne diverse dalle chiavi articolo.
    return [column for column in text_analysis_df.columns if column not in NON_METRIC_COLUMNS]


def load_granger_finbert_coefficients() -> pd.DataFrame:
    # Leggo i coefficienti FinBERT del VAR Granger prodotti a livello ticker-lag.
    if not GRANGER_FINBERT_COEFFICIENTS_FILE.exists():
        raise FileNotFoundError(
            "Missing FinBERT Granger coefficients file: "
            f"{GRANGER_FINBERT_COEFFICIENTS_FILE}"
        )

    coefficients_df = pd.read_csv(GRANGER_FINBERT_COEFFICIENTS_FILE)
    required_columns = {
        "Ticker",
        GRANGER_LAG_COLUMN,
        GRANGER_BETA_NEGATIVE_COL,
        GRANGER_BETA_NEUTRAL_COL,
        GRANGER_BETA_POSITIVE_COL,
    }
    missing_columns = required_columns.difference(coefficients_df.columns)
    if missing_columns:
        raise KeyError(
            "Missing required columns in FinBERT Granger coefficients file: "
            f"{sorted(missing_columns)}"
        )

    coefficients_df["Ticker"] = normalize_text_for_key(coefficients_df["Ticker"])
    coefficients_df[GRANGER_LAG_COLUMN] = pd.to_numeric(
        coefficients_df[GRANGER_LAG_COLUMN], errors="coerce"
    ).astype("Int64")
    coefficients_df = coefficients_df.dropna(subset=[GRANGER_LAG_COLUMN]).copy()
    coefficients_df = coefficients_df.drop_duplicates(
        subset=["Ticker", GRANGER_LAG_COLUMN], keep="last"
    )
    return coefficients_df


def build_granger_finbert_coefficient_map(coefficients_df: pd.DataFrame) -> pd.DataFrame:
    # Porto i coefficienti in wide per fare un merge semplice su ogni articolo.
    wide_df = coefficients_df.pivot(
        index="Ticker",
        columns=GRANGER_LAG_COLUMN,
        values=[
            GRANGER_BETA_NEGATIVE_COL,
            GRANGER_BETA_NEUTRAL_COL,
            GRANGER_BETA_POSITIVE_COL,
        ],
    )
    wide_df.columns = [
        f"{metric}_lag_{lag}"
        for metric, lag in wide_df.columns.to_flat_index()
    ]
    return wide_df.reset_index()


def add_finbert_sentiment_column(text_analysis_df: pd.DataFrame) -> pd.DataFrame:
    # Costruisco un indicatore sintetico articolo-per-articolo partendo dai tre
    # score FinBERT, cosi la successiva media settimanale lo aggrega insieme
    # a tutte le altre metriche news senza sostituirle.
    working_df = text_analysis_df.copy()
    finbert_cols = [FINBERT_NEGATIVE_COL, FINBERT_NEUTRAL_COL, FINBERT_POSITIVE_COL]

    has_finbert = all(column in working_df.columns for column in finbert_cols)
    if not has_finbert:
        if FINBERT_SENTIMENT_COL in working_df.columns:
            return working_df
        missing = ", ".join(finbert_cols)
        raise KeyError(
            "Missing FinBERT columns required to build article sentiment: "
            f"{missing}"
        )

    working_df[finbert_cols] = working_df[finbert_cols].apply(pd.to_numeric, errors="coerce")
    working_df[FINBERT_SENTIMENT_COL] = (
        (-1 * working_df[FINBERT_NEGATIVE_COL])
        + (0 * working_df[FINBERT_NEUTRAL_COL])
        + (1 * working_df[FINBERT_POSITIVE_COL])
    )

    coefficients_df = load_granger_finbert_coefficients()
    coefficient_map_df = build_granger_finbert_coefficient_map(coefficients_df)
    working_df["Ticker"] = normalize_text_for_key(working_df["Ticker"])
    working_df = working_df.merge(coefficient_map_df, on="Ticker", how="left")

    available_lags = sorted(coefficients_df[GRANGER_LAG_COLUMN].dropna().unique().tolist())
    for lag in available_lags:
        beta_negative_col = f"{GRANGER_BETA_NEGATIVE_COL}_lag_{lag}"
        beta_neutral_col = f"{GRANGER_BETA_NEUTRAL_COL}_lag_{lag}"
        beta_positive_col = f"{GRANGER_BETA_POSITIVE_COL}_lag_{lag}"
        article_score_col = GRANGER_ARTICLE_SCORE_TEMPLATE.format(lag=lag)

        working_df[article_score_col] = (
            working_df[beta_negative_col] * working_df[FINBERT_NEGATIVE_COL]
            + working_df[beta_neutral_col] * working_df[FINBERT_NEUTRAL_COL]
            + working_df[beta_positive_col] * working_df[FINBERT_POSITIVE_COL]
        )

    coefficient_columns = [
        column
        for column in working_df.columns
        if column.startswith(f"{GRANGER_BETA_NEGATIVE_COL}_lag_")
        or column.startswith(f"{GRANGER_BETA_NEUTRAL_COL}_lag_")
        or column.startswith(f"{GRANGER_BETA_POSITIVE_COL}_lag_")
    ]
    working_df = working_df.drop(columns=coefficient_columns, errors="ignore")
    return working_df


def build_weekly_news_features(text_analysis_df: pd.DataFrame) -> pd.DataFrame:
    # 1. Copio il dataframe per non modificare quello letto da CSV.
    working_df = add_finbert_sentiment_column(text_analysis_df)

    # 2. Porto ogni articolo nel calendario settimanale usato dal progetto.
    working_df["WeekEndingFriday"] = to_week_ending_friday(working_df["Date"])

    # 3. Elimino le righe senza ticker, senza headline o senza data interpretabile.
    working_df["Ticker"] = normalize_text_for_key(working_df["Ticker"])
    working_df["Headline"] = normalize_text_for_key(working_df["Headline"])
    working_df = working_df.dropna(subset=["WeekEndingFriday"])
    working_df = working_df[working_df["Ticker"].ne("")]
    working_df = working_df[working_df["Headline"].ne("")]

    # 4. Tengo traccia del numero di articoli grezzi prima della deduplica.
    raw_article_counts = (
        working_df.groupby(KEY_COLUMNS, as_index=False)
        .size()
        .rename(columns={"size": "NEWS_RawArticleCount"})
    )

    # 5. Deduplico headline identiche nello stesso ticker-week.
    working_df = working_df.drop_duplicates(subset=HEADLINE_DEDUP_COLUMNS, keep="first")

    # 6. Individuo le colonne metriche e le forzo a numerico.
    metric_columns = get_metric_columns(working_df)
    working_df[metric_columns] = working_df[metric_columns].apply(
        pd.to_numeric, errors="coerce"
    )

    # 7. Calcolo la media settimanale per azienda su tutte le metriche disponibili.
    weekly_metrics = (
        working_df.groupby(KEY_COLUMNS, as_index=False)[metric_columns].mean()
    )

    # 8. Aggiungo un conteggio articoli utile per controllare quanto e denso
    # il segnale news in ciascuna settimana dopo la deduplica per headline.
    article_counts = (
        working_df.groupby(KEY_COLUMNS, as_index=False)
        .size()
        .rename(columns={"size": "NEWS_ArticleCount"})
    )

    weekly_metrics = weekly_metrics.merge(article_counts, on=KEY_COLUMNS, how="left")
    weekly_metrics = weekly_metrics.merge(raw_article_counts, on=KEY_COLUMNS, how="left")

    # 9. Rinomino le metriche aggregate con un prefisso esplicito.
    rename_map = {
        column: f"{NEWS_FEATURE_PREFIX}{column}{NEWS_FEATURE_SUFFIX}"
        for column in metric_columns
    }
    weekly_metrics = weekly_metrics.rename(columns=rename_map)

    return weekly_metrics.sort_values(KEY_COLUMNS).reset_index(drop=True)


def add_granger_finbert_lagged_score(aligned_weekly_news_df: pd.DataFrame) -> pd.DataFrame:
    # Il punteggio Granger finale va costruito a livello settimanale:
    # 1. ogni articolo usa i beta del proprio ticker e lag;
    # 2. gli score articolo vengono mediati per settimana;
    # 3. la feature finale per la settimana t usa gli shift di 1, 2 e 3 settimane.
    working_df = aligned_weekly_news_df.sort_values(KEY_COLUMNS).copy()

    source_columns = [
        column
        for column in working_df.columns
        if column.startswith(f"{NEWS_FEATURE_PREFIX}FINBERT_GrangerArticleScore_Lag")
        and column.endswith(NEWS_FEATURE_SUFFIX)
    ]
    if not source_columns:
        return working_df

    shifted_columns: list[str] = []
    for source_column in sorted(source_columns):
        lag = int(source_column.split("Lag", 1)[1].split("_", 1)[0])
        shifted_column = GRANGER_WEEKLY_SHIFTED_SCORE_TEMPLATE.format(lag=lag)
        working_df[shifted_column] = (
            working_df.groupby("Ticker", sort=False)[source_column].shift(lag)
        )
        shifted_columns.append(shifted_column)

    working_df[GRANGER_FINAL_SCORE_COLUMN] = working_df[shifted_columns].sum(
        axis=1,
        min_count=len(shifted_columns),
    )

    def standardize_by_ticker(score_series: pd.Series) -> pd.Series:
        # Standardizzo il segnale finale all'interno del ticker per renderlo
        # confrontabile nel modeling mantenendo i missing dove il punteggio
        # Granger non e definito.
        valid_scores = score_series.dropna()
        if valid_scores.empty:
            return score_series

        std = valid_scores.std()
        if pd.isna(std) or std == 0:
            return score_series.where(score_series.isna(), 0.0)

        mean = valid_scores.mean()
        return (score_series - mean) / std

    working_df[GRANGER_FINAL_SCORE_COLUMN] = (
        working_df.groupby("Ticker", sort=False)[GRANGER_FINAL_SCORE_COLUMN]
        .transform(standardize_by_ticker)
    )
    return working_df


def build_modeling_dataset(full_data_with_news_df: pd.DataFrame) -> pd.DataFrame:
    # Creo il dataset modeling partendo dal merge finale fullData + news.
    # Per richiesta del progetto:
    # 1. tolgo le colonne diagnostiche di conteggio articoli;
    # 2. tengo solo le feature news FinBERT richieste per il modeling;
    # 3. elimino tutte le righe con almeno un missing;
    modeling_df = full_data_with_news_df.copy()

    modeling_df = modeling_df.drop(
        columns=["NEWS_ArticleCount", "NEWS_RawArticleCount"],
        errors="ignore",
    )

    allowed_news_columns = {
        "NEWS_FINBERT_Negative_Mean",
        "NEWS_FINBERT_Neutral_Mean",
        "NEWS_FINBERT_Positive_Mean",
        "NEWS_Sentiment_Mean",
        GRANGER_FINAL_SCORE_COLUMN,
    }

    removable_news_columns = [
        column
        for column in modeling_df.columns
        if column.startswith("NEWS_") and column not in allowed_news_columns
    ]
    modeling_df = modeling_df.drop(columns=removable_news_columns, errors="ignore")

    modeling_df = modeling_df.dropna().reset_index(drop=True)
    return modeling_df


# ---------------------------------------------------------------------------
# FLUSSO PRINCIPALE
# ---------------------------------------------------------------------------

def main():
    # -----------------------------------------------------------------------
    # 1. LETTURA DEI FILE DI INPUT
    # -----------------------------------------------------------------------

    text_analysis_df = pd.read_csv(cfg.ANALYSIS_TEXT)
    full_data_df = pd.read_csv(cfg.FULL_DATA)
    text_analysis_df = attach_headline_column(text_analysis_df)

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
    aligned_weekly_news_df = add_granger_finbert_lagged_score(aligned_weekly_news_df)

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

    # Mantengo un file intermedio solo-news gia allineato a fullData, un file
    # finale completo con le colonne news gia aggiunte e un file modeling gia
    # ripulito dai missing.
    aligned_weekly_news_to_save = aligned_weekly_news_df.copy()
    full_data_with_news_to_save = full_data_with_news_df.copy()
    modeling_df_to_save = build_modeling_dataset(full_data_with_news_to_save)

    aligned_weekly_news_to_save["WeekEndingFriday"] = aligned_weekly_news_to_save[
        "WeekEndingFriday"
    ].dt.strftime("%Y-%m-%d")
    full_data_with_news_to_save["WeekEndingFriday"] = full_data_with_news_to_save[
        "WeekEndingFriday"
    ].dt.strftime("%Y-%m-%d")

    cfg.MODELING.mkdir(parents=True, exist_ok=True)

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
    modeling_df_to_save.to_csv(
        cfg.MODELING_DATASET,
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
            "raw_article_count_sum": int(aligned_weekly_news_to_save["NEWS_RawArticleCount"].fillna(0).sum()),
            "deduplicated_article_count_sum": int(aligned_weekly_news_to_save["NEWS_ArticleCount"].fillna(0).sum()),
            "weekly_output": str(cfg.ANALYSIS_TEXT_WEEKLY),
            "merged_output": str(cfg.FULL_DATA_WITH_NEWS),
            "modeling_rows": len(modeling_df_to_save),
            "modeling_columns": len(modeling_df_to_save.columns),
            "modeling_output": str(cfg.MODELING_DATASET),
        },
    )


if __name__ == "__main__":
    main()
