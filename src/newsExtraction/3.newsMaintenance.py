import os
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg


# Credenziali Alpaca usate per il download mirato di un solo ticker.
API_KEY = os.getenv("ALPACA_API_KEY", "PKVESCM6H235I3XWBT25AYP7JC")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "6MPjbdQyG6PmWP1niRAAGPFM3HQw2SvAWMGWL3r5Q3FM")
NEWS_URL = "https://data.alpaca.markets/v1beta1/news"
HEADERS = {
    "Apca-Api-Key-Id": API_KEY,
    "Apca-Api-Secret-Key": SECRET_KEY,
    "accept": "application/json",
}


# Lista manuale di ticker da riscaricare o aggiungere dopo l'allineamento automatico.
# Se la lasci vuota, lo script fa solo l'allineamento tra enterprises.csv e newsArticles.csv.
# Esempio: MANUAL_TICKERS_TO_DOWNLOAD = ["AAPL", "MSFT"]
MANUAL_TICKERS_TO_DOWNLOAD = []

# Ticker opzionale via variabile d'ambiente.
# Se valorizzato, viene aggiunto alla lista manuale sopra.
TARGET_TICKER = os.getenv("NEWS_MAINTENANCE_TICKER", "").strip().upper()
START_DATE = os.getenv("NEWS_IMPORT_START", "2021-01-01")
END_DATE = os.getenv("NEWS_IMPORT_END", "2026-03-27")
WINDOW_DAYS = int(os.getenv("NEWS_IMPORT_WINDOW_DAYS", "120"))
API_LIMIT = int(os.getenv("NEWS_IMPORT_API_LIMIT", "50"))
MAX_REQUESTS_PER_MINUTE = int(os.getenv("NEWS_IMPORT_MAX_RPM", "200"))
MAX_REQUESTS_PER_SECOND = int(os.getenv("NEWS_IMPORT_MAX_RPS", "10"))
MAX_RETRIES = int(os.getenv("NEWS_IMPORT_MAX_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = float(os.getenv("NEWS_IMPORT_RETRY_BACKOFF_SECONDS", "2"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("NEWS_IMPORT_TIMEOUT_SECONDS", "20"))


# Colonne standard del dataset news.
OUTPUT_COLUMNS = ["ID", "Ticker", "Date", "Source", "Headline", "Summary", "Content", "URL"]
RAW_OUTPUT_DIR = cfg.NEWS_EXTRACTION / "raw_news_data"


def wait_for_rate_limit(rate_state):
    # Rispetto dei limiti per secondo e per minuto dell'API.
    now = time.time()
    elapsed = now - rate_state["last_request_time"]
    min_interval = 1.0 / MAX_REQUESTS_PER_SECOND

    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
        now = time.time()

    while rate_state["request_times"] and rate_state["request_times"][0] < now - 60:
        rate_state["request_times"].popleft()

    if len(rate_state["request_times"]) >= MAX_REQUESTS_PER_MINUTE:
        sleep_time = 60 - (now - rate_state["request_times"][0]) + 0.1
        if sleep_time > 0:
            print(f"Pausa rate limit: {sleep_time:.1f}s")
            time.sleep(sleep_time)
            now = time.time()
            while rate_state["request_times"] and rate_state["request_times"][0] < now - 60:
                rate_state["request_times"].popleft()

    rate_state["request_times"].append(now)
    rate_state["last_request_time"] = now


def validate_api_credentials():
    # Il download Alpaca e possibile solo se sono disponibili delle credenziali.
    if not API_KEY or not SECRET_KEY:
        raise EnvironmentError(
            "Variabili d'ambiente mancanti: imposta ALPACA_API_KEY e ALPACA_SECRET_KEY."
        )


def fill_missing_summaries(df):
    # Mantengo coerente il dataset: se manca il summary, uso il titolo della news.
    summary_missing_mask = df["Summary"].isna() | df["Summary"].astype(str).str.strip().eq("")
    df.loc[summary_missing_mask, "Summary"] = df.loc[summary_missing_mask, "Headline"].fillna("")
    return int(summary_missing_mask.sum())


def download_single_ticker_news(ticker):
    # Creo finestre temporali ampie per ridurre il numero di richieste.
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt_exclusive = datetime.strptime(END_DATE, "%Y-%m-%d") + timedelta(days=1)
    pending_windows = deque()
    cursor = start_dt

    while cursor < end_dt_exclusive:
        next_cursor = min(cursor + timedelta(days=WINDOW_DAYS), end_dt_exclusive)
        pending_windows.append((cursor, next_cursor))
        cursor = next_cursor

    rate_state = {"request_times": deque(), "last_request_time": 0.0}
    ticker_rows = []
    processed_windows = 0

    with requests.Session() as session:
        while pending_windows:
            window_start, window_end = pending_windows.popleft()
            processed_windows += 1

            print(
                f"{ticker} | finestra {processed_windows} | "
                f"{window_start.date()} -> {(window_end - timedelta(days=1)).date()} | "
                f"restanti {len(pending_windows)}"
            )

            params = {
                "symbols": ticker,
                "start": window_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end": window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "limit": API_LIMIT,
            }

            window_articles = []
            first_page_size = 0
            used_pagination = False
            request_ok = False
            last_error = None

            # Retry semplice per evitare di perdere una finestra per errori transitori.
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    wait_for_rate_limit(rate_state)
                    response = session.get(
                        NEWS_URL,
                        headers=HEADERS,
                        params=params,
                        timeout=REQUEST_TIMEOUT_SECONDS,
                    )
                    response.raise_for_status()

                    payload = response.json()
                    batch = payload.get("news", [])
                    first_page_size = len(batch)
                    window_articles.extend(batch)

                    # Se ci sono piu pagine, continuo fino in fondo.
                    next_page_token = payload.get("next_page_token")
                    seen_tokens = set()

                    while next_page_token:
                        used_pagination = True

                        if next_page_token in seen_tokens:
                            print(f"{ticker}: page token ripetuto, interrompo la paginazione.")
                            break

                        seen_tokens.add(next_page_token)
                        paged_params = dict(params)
                        paged_params["page_token"] = next_page_token

                        wait_for_rate_limit(rate_state)
                        paged_response = session.get(
                            NEWS_URL,
                            headers=HEADERS,
                            params=paged_params,
                            timeout=REQUEST_TIMEOUT_SECONDS,
                        )
                        paged_response.raise_for_status()

                        paged_payload = paged_response.json()
                        window_articles.extend(paged_payload.get("news", []))
                        next_page_token = paged_payload.get("next_page_token")

                    request_ok = True
                    break
                except requests.RequestException as exc:
                    last_error = exc
                    if attempt < MAX_RETRIES:
                        sleep_seconds = RETRY_BACKOFF_SECONDS * attempt
                        print(f"{ticker}: errore richiesta, retry tra {sleep_seconds:.1f}s.")
                        time.sleep(sleep_seconds)

            # Se una finestra fallisce definitivamente, fermo tutto per non salvare un dataset monco.
            if not request_ok:
                raise RuntimeError(f"Download fallito per {ticker}: {last_error}")

            # Se la finestra e troppo piena e l'API non espone paginazione, la divido in due.
            window_span = window_end - window_start
            if first_page_size == API_LIMIT and not used_pagination and window_span > timedelta(days=1):
                midpoint = window_start + window_span / 2

                if midpoint <= window_start:
                    midpoint = window_start + timedelta(hours=12)
                if midpoint >= window_end:
                    midpoint = window_end - timedelta(hours=12)

                pending_windows.appendleft((midpoint, window_end))
                pending_windows.appendleft((window_start, midpoint))
                print(f"{ticker}: finestra troppo densa, la divido in due.")
                continue

            # Normalizzo il risultato nel formato usato da newsArticles.csv.
            for article in window_articles:
                ticker_rows.append(
                    {
                        "ID": article.get("id"),
                        "Ticker": ticker,
                        "Date": article.get("created_at"),
                        "Source": article.get("source", ""),
                        "Headline": article.get("headline", ""),
                        "Summary": article.get("summary", ""),
                        "Content": article.get("content", ""),
                        "URL": article.get("url", ""),
                    }
                )

    ticker_df = pd.DataFrame(ticker_rows, columns=OUTPUT_COLUMNS)
    if not ticker_df.empty:
        ticker_df.drop_duplicates(subset=["ID", "Ticker"], inplace=True)
        ticker_df.sort_values(by=["Date", "ID"], ascending=[True, True], inplace=True)

    return ticker_df


def main():
    # Creo le cartelle dati se necessario.
    cfg.NEWS_EXTRACTION.mkdir(parents=True, exist_ok=True)
    RAW_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Carico i ticker ammessi dall'universo corrente.
    enterprises_df = pd.read_csv(cfg.ENT, usecols=["Ticker"])
    valid_tickers = enterprises_df["Ticker"].dropna().astype(str).str.strip()
    valid_tickers = valid_tickers[valid_tickers.ne("")]
    valid_ticker_set = set(valid_tickers.drop_duplicates().tolist())

    # Carico il dataset news esistente oppure ne creo uno vuoto.
    if cfg.NEWS_ARTICLES.exists():
        news_df = pd.read_csv(cfg.NEWS_ARTICLES)
    else:
        news_df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    for column in OUTPUT_COLUMNS:
        if column not in news_df.columns:
            news_df[column] = pd.NA
    news_df = news_df[OUTPUT_COLUMNS].copy()

    # Calcolo i ticker effettivamente presenti nel dataset news attuale.
    news_tickers = news_df["Ticker"].dropna().astype(str).str.strip()
    news_tickers = news_tickers[news_tickers.ne("")]
    news_ticker_set = set(news_tickers.drop_duplicates().tolist())

    # Elimino le righe di ticker che non esistono piu in enterprises.csv.
    rows_before_cleanup = len(news_df)
    news_df = news_df[news_df["Ticker"].isin(valid_ticker_set)].copy()
    removed_invalid_ticker_rows = rows_before_cleanup - len(news_df)

    # Identifico i ticker presenti in enterprises.csv ma assenti da newsArticles.csv.
    missing_in_news = sorted(valid_ticker_set - news_ticker_set)
    tickers_to_refresh = list(missing_in_news)

    # Normalizzo la lista manuale definita in alto nel file.
    manual_tickers = []
    for ticker in MANUAL_TICKERS_TO_DOWNLOAD:
        cleaned_ticker = str(ticker).strip().upper()
        if cleaned_ticker:
            manual_tickers.append(cleaned_ticker)

    # Mantengo anche il supporto opzionale via variabile d'ambiente.
    if TARGET_TICKER:
        manual_tickers.append(TARGET_TICKER)

    # Tolgo eventuali duplicati preservando l'ordine.
    manual_tickers = list(dict.fromkeys(manual_tickers))

    # Valido i ticker manuali rispetto a enterprises.csv.
    invalid_manual_tickers = [ticker for ticker in manual_tickers if ticker not in valid_ticker_set]
    if invalid_manual_tickers:
        raise ValueError(
            f"Questi ticker manuali non sono presenti in enterprises.csv: {invalid_manual_tickers}"
        )

    # Dopo l'allineamento aggiungo anche gli eventuali ticker scelti manualmente.
    for ticker in manual_tickers:
        if ticker not in tickers_to_refresh:
            tickers_to_refresh.append(ticker)

    downloaded_tickers = []
    downloaded_rows = 0
    empty_download_tickers = []
    preserved_existing_tickers = []

    if tickers_to_refresh:
        validate_api_credentials()

    # Scarico tutti i ticker mancanti nel dataset news e gli eventuali refresh espliciti.
    for ticker in tickers_to_refresh:
        print(f"Avvio aggiornamento per {ticker}")
        ticker_df = download_single_ticker_news(ticker)
        ticker_already_present = news_df["Ticker"].eq(ticker).any()

        # Se il download e vuoto ma avevo gia dati storici, li mantengo invece di cancellarli.
        if ticker_df.empty and ticker_already_present:
            print(f"{ticker}: download vuoto, mantengo le news gia presenti.")
            empty_download_tickers.append(ticker)
            preserved_existing_tickers.append(ticker)
            continue

        # Prima rimuovo tutte le righe correnti del ticker, poi inserisco il nuovo risultato.
        news_df = news_df[news_df["Ticker"] != ticker].copy()
        if not ticker_df.empty:
            news_df = pd.concat([news_df, ticker_df], ignore_index=True)

        # Aggiorno anche il file raw del ticker per mantenere allineati i file intermedi.
        if not ticker_df.empty:
            target_output_path = RAW_OUTPUT_DIR / f"{ticker}.csv"
            ticker_df.to_csv(target_output_path, index=False, encoding="utf-8-sig")

        downloaded_tickers.append(ticker)
        downloaded_rows += len(ticker_df)

        if ticker_df.empty:
            empty_download_tickers.append(ticker)

    filled_summary_from_headline = fill_missing_summaries(news_df)

    # Deduplica e ordinamento finale prima del salvataggio.
    news_df.drop_duplicates(subset=["ID", "Ticker"], inplace=True)
    news_df.sort_values(by=["Ticker", "Date"], ascending=[True, True], inplace=True)
    news_df.to_csv(cfg.NEWS_ARTICLES, index=False, encoding="utf-8-sig")

    print(
        "Manutenzione news completata:",
        {
            "rows_final": len(news_df),
            "removed_invalid_ticker_rows": int(removed_invalid_ticker_rows),
            "missing_tickers_in_news_before_update": len(missing_in_news),
            "manual_tickers_requested": manual_tickers,
            "downloaded_tickers": downloaded_tickers,
            "downloaded_rows": int(downloaded_rows),
            "empty_download_tickers": empty_download_tickers,
            "preserved_existing_tickers_after_empty_download": preserved_existing_tickers,
            "filled_summary_from_headline": int(filled_summary_from_headline),
            "forced_refresh_ticker": TARGET_TICKER or None,
            "output": str(cfg.NEWS_ARTICLES),
        },
    )


if __name__ == "__main__":
    main()
