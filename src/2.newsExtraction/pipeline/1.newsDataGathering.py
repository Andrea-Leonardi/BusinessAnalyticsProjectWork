"""
1.newsDataGathering.py

Scopo del file:
- scaricare da Alpaca le news per tutti i ticker presenti in enterprises.csv
- salvare un CSV per ogni azienda nella cartella raw_news_data
- unire tutti i file aziendali in un unico newsArticles.csv finale

Idea generale del flusso:
1. legge configurazione, ticker e intervallo temporale
2. decide quali ticker devono davvero essere scaricati
3. scarica i ticker in parallelo con un rate limiter globale condiviso
4. usa finestre temporali adattive per ridurre split inutili
5. deduplica i record e salva sia i file per ticker sia il dataset generale
"""

import os
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
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
MIN_WINDOW_DAYS = int(os.getenv("NEWS_IMPORT_MIN_WINDOW_DAYS", "15"))
MAX_DOWNLOAD_WORKERS = int(os.getenv("NEWS_IMPORT_MAX_WORKERS", "4"))
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

RAW_OUTPUT_DIR = cfg.RAW_NEWS_DATA
RAW_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
cfg.NEWS_EXTRACTION.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# RATE LIMITER GLOBALE CONDIVISO TRA I WORKER
# ---------------------------------------------------------------------------

class SharedRateLimiter:
    # Questo oggetto coordina tutti i thread cosi il limite Alpaca resta
    # rispettato anche quando piu ticker stanno scaricando insieme.

    def __init__(self):
        self.request_times = deque()
        self.last_request_time = 0.0
        self.lock = Lock()

    def wait_for_slot(self):
        # Ogni richiesta passa di qui prima di partire.
        while True:
            sleep_seconds = 0.0

            with self.lock:
                now = time.time()
                min_interval = 1.0 / MAX_REQUESTS_PER_SECOND
                elapsed = now - self.last_request_time

                while self.request_times and self.request_times[0] < now - 60:
                    self.request_times.popleft()

                if elapsed < min_interval:
                    sleep_seconds = max(sleep_seconds, min_interval - elapsed)

                if len(self.request_times) >= MAX_REQUESTS_PER_MINUTE:
                    minute_sleep = 60 - (now - self.request_times[0]) + 0.1
                    sleep_seconds = max(sleep_seconds, minute_sleep)

                if sleep_seconds <= 0:
                    now = time.time()

                    while self.request_times and self.request_times[0] < now - 60:
                        self.request_times.popleft()

                    self.request_times.append(now)
                    self.last_request_time = now
                    return

            time.sleep(sleep_seconds)


# ---------------------------------------------------------------------------
# FUNZIONI DI SUPPORTO GENERALI
# ---------------------------------------------------------------------------

def validate_api_credentials():
    # Verifica che esistano credenziali usabili per i download.
    if not API_KEY or not SECRET_KEY:
        raise EnvironmentError(
            "Variabili d'ambiente mancanti: imposta ALPACA_API_KEY e ALPACA_SECRET_KEY."
        )


def deduplicate_news_df(df):
    # Separo le righe con ID valido da quelle senza ID.
    # Le prime si deduplicano su ID + Ticker, le seconde sul fallback testuale.
    # Questo evita che tutti gli articoli con ID mancante collassino in una sola
    # riga per ticker durante il merge finale.
    if df.empty:
        return df

    if "ID" not in df.columns:
        return df.drop_duplicates(subset=FALLBACK_DEDUP_COLUMNS)

    id_as_text = df["ID"].astype(str).str.strip().str.lower()
    valid_id_mask = df["ID"].notna() & ~id_as_text.isin({"", "nan", "none", "<na>"})

    with_id_df = df.loc[valid_id_mask].drop_duplicates(subset=PRIMARY_DEDUP_COLUMNS)
    without_id_df = df.loc[~valid_id_mask].drop_duplicates(subset=FALLBACK_DEDUP_COLUMNS)

    return pd.concat([with_id_df, without_id_df], ignore_index=True, sort=False)


def load_target_tickers():
    # Legge il perimetro aziende e applica eventuali filtri di debug.
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

    return tickers


def get_initial_window_days():
    # Evito configurazioni incoerenti tra finestra minima e massima.
    return max(WINDOW_DAYS, MIN_WINDOW_DAYS)


def get_shrunk_window_days(current_window_days):
    # Quando una finestra e troppo densa la riduco in modo deciso ma senza
    # scendere sotto il minimo consentito.
    shrunk_window_days = max(MIN_WINDOW_DAYS, current_window_days // 2)

    if shrunk_window_days >= current_window_days and current_window_days > MIN_WINDOW_DAYS:
        shrunk_window_days = current_window_days - 1

    return max(MIN_WINDOW_DAYS, shrunk_window_days)


def get_expanded_window_days(current_window_days):
    # Quando una finestra e molto vuota, allarghiamo le successive per
    # ridurre il numero totale di richieste.
    return min(get_initial_window_days(), current_window_days * 2)


def build_article_rows(ticker, articles):
    # Normalizza gli articoli Alpaca nello schema usato dal progetto.
    rows = []

    for article in articles:
        rows.append(
            {
                "ID": article.get("id"),
                "Ticker": ticker,
                "Date": article.get("created_at"),
                "Headline": article.get("headline", ""),
                "Summary": article.get("summary", ""),
            }
        )

    return rows


# ---------------------------------------------------------------------------
# RICHIESTE API E PAGINAZIONE
# ---------------------------------------------------------------------------

def fetch_window_articles(session, ticker, window_start, window_end, rate_limiter):
    # Scarica una singola finestra temporale e segue tutta la paginazione.
    params = {
        "symbols": ticker,
        "start": window_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "limit": API_LIMIT,
    }

    articles = []
    first_page_size = 0
    used_pagination = False
    page_count = 0
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            rate_limiter.wait_for_slot()
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
            articles.extend(batch)
            page_count = 1

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

                rate_limiter.wait_for_slot()
                paged_response = session.get(
                    NEWS_URL,
                    headers=HEADERS,
                    params=paged_params,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                paged_response.raise_for_status()

                paged_payload = paged_response.json()
                articles.extend(paged_payload.get("news", []))
                next_page_token = paged_payload.get("next_page_token")
                page_count += 1

            return {
                "ok": True,
                "articles": articles,
                "first_page_size": first_page_size,
                "used_pagination": used_pagination,
                "page_count": page_count,
            }
        except requests.RequestException as exc:
            last_error = exc

            if attempt < MAX_RETRIES:
                sleep_seconds = RETRY_BACKOFF_SECONDS * attempt
                print(f"{ticker}: errore richiesta, retry tra {sleep_seconds:.1f}s.")
                time.sleep(sleep_seconds)

    return {
        "ok": False,
        "articles": [],
        "first_page_size": first_page_size,
        "used_pagination": used_pagination,
        "page_count": page_count,
        "error": last_error,
    }


# ---------------------------------------------------------------------------
# DOWNLOAD DEL SINGOLO TICKER CON FINESTRE ADATTIVE
# ---------------------------------------------------------------------------

def download_ticker_news(ticker, ticker_index, total_tickers, start_dt, end_dt_exclusive, rate_limiter):
    # Ogni worker gestisce un ticker intero in autonomia.
    output_path = RAW_OUTPUT_DIR / f"{ticker}.csv"
    ticker_rows = []
    current_start = start_dt
    current_window_days = get_initial_window_days()
    completed_windows = 0
    retried_dense_windows = 0

    with requests.Session() as session:
        while current_start < end_dt_exclusive:
            window_end = min(current_start + timedelta(days=current_window_days), end_dt_exclusive)
            window_span_days = max(1, (window_end - current_start).days)
            completed_windows += 1

            print(
                f"[{ticker_index}/{total_tickers}] {ticker} | "
                f"finestra {completed_windows} | "
                f"{current_start.date()} -> {(window_end - timedelta(days=1)).date()} | "
                f"giorni {window_span_days}"
            )

            result = fetch_window_articles(
                session=session,
                ticker=ticker,
                window_start=current_start,
                window_end=window_end,
                rate_limiter=rate_limiter,
            )

            if not result["ok"]:
                print(f"{ticker}: finestra saltata per errore -> {result['error']}")
                current_start = window_end
                continue

            first_page_size = result["first_page_size"]
            used_pagination = result["used_pagination"]
            window_articles = result["articles"]

            # Se la prima pagina e piena ma Alpaca non fornisce paginazione,
            # la finestra potrebbe essere ambigua. In quel caso la ritento con
            # una granularita piu piccola invece di accettare un possibile taglio.
            ambiguous_saturation = first_page_size == API_LIMIT and not used_pagination
            can_shrink = window_span_days > MIN_WINDOW_DAYS

            if ambiguous_saturation and can_shrink:
                new_window_days = get_shrunk_window_days(window_span_days)
                retried_dense_windows += 1
                current_window_days = new_window_days
                print(
                    f"{ticker}: finestra densa senza paginazione, "
                    f"riduco da {window_span_days} a {new_window_days} giorni e ritento."
                )
                continue

            ticker_rows.extend(build_article_rows(ticker, window_articles))

            # Dopo una finestra poco densa posso allargare le successive.
            # Dopo una finestra al limite ma non ambigua mantengo prudenza.
            if not used_pagination and first_page_size == 0 and current_window_days < get_initial_window_days():
                current_window_days = get_expanded_window_days(current_window_days)
            elif not used_pagination and first_page_size <= max(1, API_LIMIT // 4):
                current_window_days = get_expanded_window_days(current_window_days)
            elif ambiguous_saturation and not can_shrink:
                current_window_days = MIN_WINDOW_DAYS

            current_start = window_end

    ticker_news = pd.DataFrame(ticker_rows, columns=OUTPUT_COLUMNS)

    if not ticker_news.empty:
        ticker_news = deduplicate_news_df(ticker_news)
        ticker_news.sort_values(by=["Date", "ID"], ascending=[True, True], inplace=True)

    ticker_news.to_csv(output_path, index=False, encoding="utf-8-sig")

    return {
        "ticker": ticker,
        "rows": len(ticker_news),
        "windows": completed_windows,
        "dense_retries": retried_dense_windows,
        "output_path": output_path,
    }


# ---------------------------------------------------------------------------
# MERGE FINALE DEI FILE AZIENDALI
# ---------------------------------------------------------------------------

def merge_company_files(tickers):
    # Ricostruisco il dataset finale solo dai ticker del perimetro corrente,
    # cosi eventuali file vecchi fuori universo non entrano nel merge.
    raw_files = [RAW_OUTPUT_DIR / f"{ticker}.csv" for ticker in tickers if (RAW_OUTPUT_DIR / f"{ticker}.csv").exists()]

    if not raw_files:
        print("Nessun file ticker disponibile, merge finale saltato.")
        return

    merged_news = pd.concat((pd.read_csv(path) for path in raw_files), ignore_index=True)
    merged_news = merged_news[OUTPUT_COLUMNS].copy()

    if not merged_news.empty:
        merged_news = deduplicate_news_df(merged_news)
        merged_news.sort_values(by=["Ticker", "Date"], ascending=[True, True], inplace=True)

    merged_news.to_csv(cfg.NEWS_ARTICLES, index=False, encoding="utf-8-sig")
    print(f"Dataset finale aggiornato in {cfg.NEWS_ARTICLES}")


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
    # 2. LETTURA DEI TICKER E PIANO DI LAVORO
    # -----------------------------------------------------------------------

    tickers = load_target_tickers()
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt_exclusive = datetime.strptime(END_DATE, "%Y-%m-%d") + timedelta(days=1)

    tickers_to_download = []
    cached_tickers = 0

    for ticker in tickers:
        output_path = RAW_OUTPUT_DIR / f"{ticker}.csv"

        if output_path.exists() and not FORCE_REFRESH:
            cached_tickers += 1
            continue

        tickers_to_download.append(ticker)

    print(
        "Avvio importazione news:",
        {
            "tickers_total": len(tickers),
            "tickers_to_download": len(tickers_to_download),
            "cached_tickers": cached_tickers,
            "start": START_DATE,
            "end": END_DATE,
            "max_window_days": WINDOW_DAYS,
            "min_window_days": MIN_WINDOW_DAYS,
            "max_workers": MAX_DOWNLOAD_WORKERS,
            "force_refresh": FORCE_REFRESH,
        },
    )

    if tickers_to_download:
        validate_api_credentials()

    # -----------------------------------------------------------------------
    # 3. DOWNLOAD PARALLELO DEI TICKER
    # -----------------------------------------------------------------------

    if tickers_to_download:
        rate_limiter = SharedRateLimiter()
        download_results = []

        with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
            futures = {
                executor.submit(
                    download_ticker_news,
                    ticker=ticker,
                    ticker_index=index,
                    total_tickers=len(tickers_to_download),
                    start_dt=start_dt,
                    end_dt_exclusive=end_dt_exclusive,
                    rate_limiter=rate_limiter,
                ): ticker
                for index, ticker in enumerate(tickers_to_download, start=1)
            }

            for future in as_completed(futures):
                ticker = futures[future]

                try:
                    result = future.result()
                    download_results.append(result)
                    print(
                        f"{result['ticker']}: salvati {result['rows']} articoli in "
                        f"{result['output_path'].name} "
                        f"(finestre {result['windows']}, retry densi {result['dense_retries']})."
                    )
                except Exception as exc:  # pragma: no cover
                    print(f"{ticker}: download fallito -> {exc}")

    # -----------------------------------------------------------------------
    # 4. MERGE FINALE DEI FILE AZIENDALI
    # -----------------------------------------------------------------------

    merge_company_files(tickers)


if __name__ == "__main__":
    main()
