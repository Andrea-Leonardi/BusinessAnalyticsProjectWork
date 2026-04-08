"""
1.newsDataGathering.py

Scopo del file:
- scaricare da Alpaca le news per tutti i ticker presenti in enterprises.csv
- salvare un CSV per ogni azienda nella cartella raw_news_data
- unire tutti i file aziendali in un unico newsArticles.csv finale

Idea generale del flusso:
1. legge configurazione, ticker e intervallo temporale
2. divide il periodo in finestre temporali piu grandi del giorno singolo
3. scarica le news rispettando i limiti dell'API
4. se una finestra e troppo piena la divide in due per non perdere articoli
5. deduplica i record e salva sia i file per ticker sia il dataset generale
"""

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


# ---------------------------------------------------------------------------
# API ALPACA E PARAMETRI DI DOWNLOAD
# ---------------------------------------------------------------------------

API_KEY = os.getenv("ALPACA_API_KEY", "PKVESCM6H235I3XWBT25AYP7JC")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "6MPjbdQyG6PmWP1niRAAGPFM3HQw2SvAWMGWL3r5Q3FM")
NEWS_URL = "https://data.alpaca.markets/v1beta1/news"
HEADERS = {
    "Apca-Api-Key-Id": API_KEY,
    "Apca-Api-Secret-Key": SECRET_KEY,
    "accept": "application/json",
}

START_DATE = os.getenv("NEWS_IMPORT_START", "2021-01-01")
END_DATE = os.getenv("NEWS_IMPORT_END", "2026-03-27")
WINDOW_DAYS = int(os.getenv("NEWS_IMPORT_WINDOW_DAYS", "120"))
API_LIMIT = int(os.getenv("NEWS_IMPORT_API_LIMIT", "50"))
MAX_REQUESTS_PER_MINUTE = int(os.getenv("NEWS_IMPORT_MAX_RPM", "200"))
MAX_REQUESTS_PER_SECOND = int(os.getenv("NEWS_IMPORT_MAX_RPS", "10"))
MAX_RETRIES = int(os.getenv("NEWS_IMPORT_MAX_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = float(os.getenv("NEWS_IMPORT_RETRY_BACKOFF_SECONDS", "2"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("NEWS_IMPORT_TIMEOUT_SECONDS", "20"))
TICKER_OFFSET = int(os.getenv("NEWS_IMPORT_TICKER_OFFSET", "0"))
TICKER_LIMIT = int(os.getenv("NEWS_IMPORT_TICKER_LIMIT", "0")) or None
FORCE_REFRESH = os.getenv("NEWS_IMPORT_FORCE_REFRESH", "0").lower() in {"1", "true", "yes"}


# ---------------------------------------------------------------------------
# SCHEMA DEI FILE E CARTELLE DI OUTPUT
# ---------------------------------------------------------------------------

OUTPUT_COLUMNS = ["ID", "Ticker", "Date", "Headline", "Summary"]
PRIMARY_DEDUP_COLUMNS = ["ID", "Ticker"]
FALLBACK_DEDUP_COLUMNS = ["Ticker", "Date", "Headline", "Summary"]

RAW_OUTPUT_DIR = cfg.NEWS_EXTRACTION / "raw_news_data"
RAW_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
cfg.NEWS_EXTRACTION.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# FUNZIONI DI SUPPORTO
# ---------------------------------------------------------------------------

def wait_for_rate_limit(rate_state):
    # Tiene il ritmo delle richieste entro i limiti dell'API.
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
    # Verifica che esistano credenziali usabili per i download.
    if not API_KEY or not SECRET_KEY:
        raise EnvironmentError(
            "Variabili d'ambiente mancanti: imposta ALPACA_API_KEY e ALPACA_SECRET_KEY."
        )


def deduplicate_news_df(df):
    # Se gli ID esistono, uso la chiave forte ID + Ticker.
    # Se manca ID, uso il fallback testuale per non perdere compatibilita.
    if df.empty:
        return df

    if "ID" in df.columns and df["ID"].notna().any():
        return df.drop_duplicates(subset=PRIMARY_DEDUP_COLUMNS)

    return df.drop_duplicates(subset=FALLBACK_DEDUP_COLUMNS)


# ---------------------------------------------------------------------------
# FLUSSO PRINCIPALE
# ---------------------------------------------------------------------------

def main():
    # -----------------------------------------------------------------------
    # 1. PREPARAZIONE DEL DATASET FINALE
    # -----------------------------------------------------------------------

    if cfg.NEWS_ARTICLES.exists():
        cfg.NEWS_ARTICLES.unlink()
        print(f"Rimosso dataset finale precedente: {cfg.NEWS_ARTICLES}")

    # -----------------------------------------------------------------------
    # 2. LETTURA DEI TICKER
    # -----------------------------------------------------------------------

    try:
        ticker_df = pd.read_csv(cfg.ENT, usecols=["Ticker"])
        tickers = ticker_df["Ticker"].dropna().astype(str).str.strip()
        tickers = tickers[tickers.ne("")].drop_duplicates().tolist()
    except FileNotFoundError:
        tickers = ["AAPL", "MSFT", "AMZN", "GOOG", "META"]
        print("enterprises.csv non trovato: uso un piccolo set di ticker di test.")

    if TICKER_OFFSET:
        tickers = tickers[TICKER_OFFSET:]
    if TICKER_LIMIT is not None:
        tickers = tickers[:TICKER_LIMIT]

    # -----------------------------------------------------------------------
    # 3. COSTRUZIONE DELLE FINESTRE TEMPORALI
    # -----------------------------------------------------------------------

    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt_exclusive = datetime.strptime(END_DATE, "%Y-%m-%d") + timedelta(days=1)
    initial_windows = []
    cursor = start_dt

    while cursor < end_dt_exclusive:
        next_cursor = min(cursor + timedelta(days=WINDOW_DAYS), end_dt_exclusive)
        initial_windows.append((cursor, next_cursor))
        cursor = next_cursor

    print(
        "Avvio importazione news:",
        {
            "tickers": len(tickers),
            "windows": len(initial_windows),
            "start": START_DATE,
            "end": END_DATE,
            "force_refresh": FORCE_REFRESH,
        },
    )

    download_required = FORCE_REFRESH or any(
        not (RAW_OUTPUT_DIR / f"{ticker}.csv").exists() for ticker in tickers
    )
    if download_required:
        validate_api_credentials()

    rate_state = {"request_times": deque(), "last_request_time": 0.0}

    # -----------------------------------------------------------------------
    # 4. DOWNLOAD TICKER PER TICKER
    # -----------------------------------------------------------------------

    with requests.Session() as session:
        for ticker_index, ticker in enumerate(tickers, start=1):
            output_path = RAW_OUTPUT_DIR / f"{ticker}.csv"

            if output_path.exists() and not FORCE_REFRESH:
                print(f"[{ticker_index}/{len(tickers)}] {ticker}: file gia presente, salto.")
                continue

            ticker_rows = []
            pending_windows = deque(initial_windows)
            processed_windows = 0

            while pending_windows:
                window_start, window_end = pending_windows.popleft()
                processed_windows += 1

                print(
                    f"[{ticker_index}/{len(tickers)}] {ticker} | "
                    f"finestra {processed_windows} | "
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

                # -------------------------------------------------------------------
                # 4A. RICHIESTA API CON RETRY E PAGINAZIONE
                # -------------------------------------------------------------------

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

                if not request_ok:
                    print(f"{ticker}: finestra saltata per errore -> {last_error}")
                    continue

                # -------------------------------------------------------------------
                # 4B. SPLIT DELLE FINESTRE TROPPO DENSE
                # -------------------------------------------------------------------

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

                # -------------------------------------------------------------------
                # 4C. NORMALIZZAZIONE DEGLI ARTICOLI
                # -------------------------------------------------------------------

                for article in window_articles:
                    ticker_rows.append(
                        {
                            "ID": article.get("id"),
                            "Ticker": ticker,
                            "Date": article.get("created_at"),
                            "Headline": article.get("headline", ""),
                            "Summary": article.get("summary", ""),
                        }
                    )

            # -----------------------------------------------------------------------
            # 4D. SALVATAGGIO DEL SINGOLO TICKER
            # -----------------------------------------------------------------------

            ticker_news = pd.DataFrame(ticker_rows, columns=OUTPUT_COLUMNS)

            if not ticker_news.empty:
                ticker_news = deduplicate_news_df(ticker_news)
                ticker_news.sort_values(by=["Date", "ID"], ascending=[True, True], inplace=True)

            ticker_news.to_csv(output_path, index=False, encoding="utf-8-sig")
            print(f"{ticker}: salvati {len(ticker_news)} articoli in {output_path.name}.")

    # -----------------------------------------------------------------------
    # 5. MERGE FINALE DEI FILE AZIENDALI
    # -----------------------------------------------------------------------

    all_files = sorted(RAW_OUTPUT_DIR.glob("*.csv"))
    if not all_files:
        print("Nessun file ticker disponibile, merge finale saltato.")
        return

    merged_news = pd.concat((pd.read_csv(path) for path in all_files), ignore_index=True)
    merged_news = merged_news[OUTPUT_COLUMNS].copy()

    if not merged_news.empty:
        merged_news = deduplicate_news_df(merged_news)
        merged_news.sort_values(by=["Ticker", "Date"], ascending=[True, True], inplace=True)

    merged_news.to_csv(cfg.NEWS_ARTICLES, index=False, encoding="utf-8-sig")
    print(f"Dataset finale aggiornato in {cfg.NEWS_ARTICLES}")


if __name__ == "__main__":
    main()
