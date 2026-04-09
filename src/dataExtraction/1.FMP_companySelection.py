#%%
"""
Selezione dell'universo aziende a partire da FMP.

Logica economica del file:
1. scarico un universo ampio di aziende USA attive;
2. aggiungo, quando disponibili sul piano FMP, anche le aziende delistate dopo
   l'inizio del campione, per ridurre survivorship bias;
3. sostituisco la market cap corrente con la market cap storica osservata
   vicino all'inizio del campione;
4. scelgo le top aziende per settore usando quella market cap storica.

Ottimizzazione principale:
- le chiamate piu costose verso FMP non vengono piu fatte tutte in serie;
- i download di profili delisted e historical market cap ora girano in
  parallelo;
- un rate limiter globale condiviso garantisce comunque il rispetto del limite
  di 300 richieste al minuto.
"""

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config as cfg


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FMP_API_BASE_URL = "https://financialmodelingprep.com/stable"
COMPANY_SCREENER_URL = f"{FMP_API_BASE_URL}/company-screener"
DELISTED_COMPANIES_URL = f"{FMP_API_BASE_URL}/delisted-companies"
PROFILE_URL = f"{FMP_API_BASE_URL}/profile"
HISTORICAL_MARKET_CAP_URL = f"{FMP_API_BASE_URL}/historical-market-capitalization"

FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
US_EXCHANGES = ["NASDAQ", "NYSE"]

# Use the first trading week of the sample as the reference point for company
# ranking, so the universe is selected with information available at the
# beginning of 2021 rather than with today's market cap.
REFERENCE_MARKET_CAP_DATE = pd.Timestamp("2021-01-04")
MARKET_CAP_LOOKBACK_DATE = "2020-12-28"
MARKET_CAP_LOOKFORWARD_DATE = "2021-01-15"

# Pull a broad active universe first, then rerank it with historical market cap.
ACTIVE_CANDIDATE_BUFFER_PER_SECTOR = 100
FINAL_COMPANIES_PER_SECTOR = 10

DELISTED_PAGE_SIZE = 100
MAX_DELISTED_PAGES = 100
REQUEST_TIMEOUT = 30
MAX_REQUEST_ATTEMPTS = 5
RATE_LIMIT_WAIT_SECONDS = 5.0

# FMP plan confirmed by user: 300 requests per minute.
MAX_REQUESTS_PER_MINUTE = 300

# Worker count for the parallel sections. The rate limiter prevents the code
# from exceeding the API limit even when several threads are active.
PROFILE_MAX_WORKERS = 6
HISTORICAL_MARKET_CAP_MAX_WORKERS = 8

# Remove names that are known to create downstream data issues in the current
# pipeline and sample design.
EXCLUDED_TICKERS = {
    "GEV",
    "TBB",
    "RCB",
    "PLTR",
    "HSBC",
    "BAC",
    "JPM",
    "WFC",
    "MUFG",
    "CTA-PA",
    "HDB",
    "PUK",
    "NGG",
    "BBDO",
    "SOJE"
}


# ---------------------------------------------------------------------------
# Shared HTTP Helpers
# ---------------------------------------------------------------------------

class GlobalRateLimiter:
    # Rate limiter semplice e thread-safe: lascia passare al massimo N richieste
    # in una finestra mobile di 60 secondi.
    def __init__(self, max_requests_per_minute: int) -> None:
        self.max_requests_per_minute = max_requests_per_minute
        self.window_seconds = 60.0
        self.request_timestamps: list[float] = []
        self.lock = threading.Lock()

    def wait_for_slot(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                cutoff = now - self.window_seconds
                self.request_timestamps = [
                    timestamp
                    for timestamp in self.request_timestamps
                    if timestamp > cutoff
                ]

                if len(self.request_timestamps) < self.max_requests_per_minute:
                    self.request_timestamps.append(now)
                    return

                earliest_timestamp = self.request_timestamps[0]
                wait_seconds = max(0.01, self.window_seconds - (now - earliest_timestamp))

            time.sleep(wait_seconds)


rate_limiter = GlobalRateLimiter(MAX_REQUESTS_PER_MINUTE)
thread_local = threading.local()


def get_thread_session() -> requests.Session:
    # Creo una sessione requests per thread cosi da riusare la connessione senza
    # condividere in modo rischioso la stessa Session tra thread diversi.
    session = getattr(thread_local, "session", None)
    if session is None:
        session = requests.Session()
        thread_local.session = session
    return session


def fetch_json(
    url: str,
    params: dict[str, object],
    context: str,
) -> list[dict] | dict:
    # Download di un payload FMP con retry e rate limit globale condiviso.
    session = get_thread_session()

    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        rate_limiter.wait_for_slot()
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    wait_seconds = float(retry_after)
                except ValueError:
                    wait_seconds = RATE_LIMIT_WAIT_SECONDS * attempt
            else:
                wait_seconds = RATE_LIMIT_WAIT_SECONDS * attempt

            print(
                f"Rate limit for {context} "
                f"(attempt {attempt}/{MAX_REQUEST_ATTEMPTS}). "
                f"Waiting {wait_seconds:.1f} seconds before retrying..."
            )
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        return response.json()

    raise requests.HTTPError(
        f"429 Too Many Requests for {context} after {MAX_REQUEST_ATTEMPTS} attempts."
    )


# ---------------------------------------------------------------------------
# Delisted Profile Helpers
# ---------------------------------------------------------------------------

def load_delisted_profile(row: pd.Series | object) -> dict[str, object] | None:
    # Recupero il profilo di una singola azienda delisted per completare il
    # settore e filtrare ETF/funds.
    payload = fetch_json(
        url=PROFILE_URL,
        params={
            "apikey": FMP_API_KEY,
            "symbol": row.symbol,
        },
        context=f"profile {row.symbol}",
    )

    if not isinstance(payload, list) or not payload:
        return None

    profile = payload[0]
    if profile.get("isEtf") or profile.get("isFund"):
        return None

    if not profile.get("sector"):
        return None

    return {
        "symbol": row.symbol,
        "companyName": profile.get("companyName") or row.companyName,
        "sector": profile.get("sector"),
        "industry": profile.get("industry"),
        "exchangeShortName": profile.get("exchange") or row.exchange,
        "universeSource": "post_2021_delisted",
    }


def load_delisted_profiles_parallel(delisted_df: pd.DataFrame) -> pd.DataFrame:
    # Scarico i profili delle delisted in parallelo per saturare il limite API
    # senza farne una coda completamente seriale.
    if delisted_df.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "companyName",
                "sector",
                "industry",
                "exchangeShortName",
                "universeSource",
            ]
        )

    delisted_rows: list[dict[str, object]] = []
    total_delisted = len(delisted_df)

    with ThreadPoolExecutor(max_workers=PROFILE_MAX_WORKERS) as executor:
        future_to_symbol = {}

        for index, row in enumerate(delisted_df.itertuples(index=False), start=1):
            print(f"Queue delisted profile for {row.symbol} ({index}/{total_delisted})...")
            future = executor.submit(load_delisted_profile, row)
            future_to_symbol[future] = row.symbol

        for completed_index, future in enumerate(as_completed(future_to_symbol), start=1):
            symbol = future_to_symbol[future]
            try:
                profile_row = future.result()
            except requests.RequestException as exc:
                print(f"Network error for delisted profile {symbol}: {exc}")
                continue
            except ValueError as exc:
                print(f"Data error for delisted profile {symbol}: {exc}")
                continue

            if profile_row is not None:
                delisted_rows.append(profile_row)

            if completed_index % 25 == 0 or completed_index == total_delisted:
                print(f"Loaded delisted profiles: {completed_index}/{total_delisted}")

    if not delisted_rows:
        return pd.DataFrame(
            columns=[
                "symbol",
                "companyName",
                "sector",
                "industry",
                "exchangeShortName",
                "universeSource",
            ]
        )

    return pd.DataFrame(delisted_rows)


# ---------------------------------------------------------------------------
# Historical Market Cap Helpers
# ---------------------------------------------------------------------------

def load_historical_market_cap_row(row: object) -> dict[str, object] | None:
    # Recupero la market cap storica usata per la classifica finale a inizio campione.
    payload = fetch_json(
        url=HISTORICAL_MARKET_CAP_URL,
        params={
            "apikey": FMP_API_KEY,
            "symbol": row.symbol,
            "from": MARKET_CAP_LOOKBACK_DATE,
            "to": MARKET_CAP_LOOKFORWARD_DATE,
        },
        context=f"historical market cap {row.symbol}",
    )

    market_cap_df = pd.DataFrame(payload)
    if market_cap_df.empty or "date" not in market_cap_df.columns:
        return None

    market_cap_df["date"] = pd.to_datetime(market_cap_df["date"], errors="coerce")
    market_cap_df["marketCap"] = pd.to_numeric(
        market_cap_df["marketCap"],
        errors="coerce",
    )
    market_cap_df = market_cap_df.dropna(subset=["date", "marketCap"]).copy()
    if market_cap_df.empty:
        return None

    # Prefer the first market-cap observation on or after the reference
    # date. If it does not exist, use the closest observation before it.
    on_or_after = market_cap_df[
        market_cap_df["date"] >= REFERENCE_MARKET_CAP_DATE
    ].sort_values("date", ascending=True)
    if not on_or_after.empty:
        selected_market_cap_row = on_or_after.iloc[0]
    else:
        before_reference = market_cap_df[
            market_cap_df["date"] < REFERENCE_MARKET_CAP_DATE
        ].sort_values("date", ascending=False)
        if before_reference.empty:
            return None
        selected_market_cap_row = before_reference.iloc[0]

    return {
        "symbol": row.symbol,
        "companyName": row.companyName,
        "sector": row.sector,
        "industry": row.industry,
        "exchangeShortName": row.exchangeShortName,
        "marketCap": float(selected_market_cap_row["marketCap"]),
        "historicalMarketCapDate": selected_market_cap_row["date"].strftime("%Y-%m-%d"),
        "selectionReferenceDate": REFERENCE_MARKET_CAP_DATE.strftime("%Y-%m-%d"),
        "universeSource": row.universeSource,
    }


def load_historical_market_caps_parallel(candidate_df: pd.DataFrame) -> pd.DataFrame:
    # Scarico la market cap storica dei candidati in parallelo, che e il tratto
    # piu costoso dell'intero script.
    if candidate_df.empty:
        return pd.DataFrame()

    historical_rows: list[dict[str, object]] = []
    total_candidates = len(candidate_df)

    with ThreadPoolExecutor(max_workers=HISTORICAL_MARKET_CAP_MAX_WORKERS) as executor:
        future_to_symbol = {}

        for index, row in enumerate(candidate_df.itertuples(index=False), start=1):
            print(
                f"Queue historical market cap for {row.symbol} "
                f"({index}/{total_candidates})..."
            )
            future = executor.submit(load_historical_market_cap_row, row)
            future_to_symbol[future] = row.symbol

        for completed_index, future in enumerate(as_completed(future_to_symbol), start=1):
            symbol = future_to_symbol[future]
            try:
                historical_row = future.result()
            except requests.RequestException as exc:
                print(f"Network error for historical market cap {symbol}: {exc}")
                continue
            except ValueError as exc:
                print(f"Data error for historical market cap {symbol}: {exc}")
                continue

            if historical_row is not None:
                historical_rows.append(historical_row)

            if completed_index % 50 == 0 or completed_index == total_candidates:
                print(
                    f"Loaded historical market caps: "
                    f"{completed_index}/{total_candidates}"
                )

    return pd.DataFrame(historical_rows)


# ---------------------------------------------------------------------------
# Download A Broad Active US Universe
# ---------------------------------------------------------------------------

active_frames: list[pd.DataFrame] = []

for exchange in US_EXCHANGES:
    print(f"Downloading active screener universe for {exchange}...")

    payload = fetch_json(
        url=COMPANY_SCREENER_URL,
        params={
            "apikey": FMP_API_KEY,
            "exchange": exchange,
            "isEtf": False,
            "isFund": False,
            "isActivelyTrading": True,
            "includeAllShareClasses": False,
            "limit": 5000,
        },
        context=f"active company screener ({exchange})",
    )

    exchange_df = pd.DataFrame(payload)
    if exchange_df.empty:
        continue

    active_frames.append(exchange_df)

if not active_frames:
    raise ValueError("No active company universe was downloaded from FMP.")

active_candidates = pd.concat(active_frames, ignore_index=True)
active_candidates = active_candidates.dropna(
    subset=["symbol", "companyName", "sector"]
).copy()
active_candidates = active_candidates[
    active_candidates["symbol"].astype(str).str.strip() != ""
]
active_candidates = active_candidates[
    ~active_candidates["symbol"].isin(EXCLUDED_TICKERS)
]
active_candidates = active_candidates.sort_values(
    "marketCap",
    ascending=False,
)

# Keep a large sector-level buffer here, because the final ranking will use
# historical market cap rather than the current market cap from the screener.
active_candidates = (
    active_candidates.groupby("sector", group_keys=False)
    .head(ACTIVE_CANDIDATE_BUFFER_PER_SECTOR)
    .copy()
)
active_candidates["universeSource"] = "active_screener"

active_candidates = active_candidates[
    [
        "symbol",
        "companyName",
        "sector",
        "industry",
        "exchangeShortName",
        "universeSource",
    ]
]


# ---------------------------------------------------------------------------
# Add Delisted US Companies When The API Plan Allows It
# ---------------------------------------------------------------------------

delisted_pages: list[pd.DataFrame] = []

for page in range(MAX_DELISTED_PAGES):
    try:
        payload = fetch_json(
            url=DELISTED_COMPANIES_URL,
            params={
                "apikey": FMP_API_KEY,
                "from": REFERENCE_MARKET_CAP_DATE.strftime("%Y-%m-%d"),
                "to": "2026-12-31",
                "page": page,
                "limit": DELISTED_PAGE_SIZE,
            },
            context=f"delisted companies page {page}",
        )
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code == 402 and page > 0:
            print(
                "FMP delisted pagination is not fully available on the current plan. "
                "Using the delisted rows that were accessible and continuing."
            )
            break
        raise

    page_df = pd.DataFrame(payload)
    if page_df.empty:
        break

    delisted_pages.append(page_df)

    if len(page_df) < DELISTED_PAGE_SIZE:
        break

delisted_candidates = pd.DataFrame(
    columns=[
        "symbol",
        "companyName",
        "sector",
        "industry",
        "exchangeShortName",
        "universeSource",
    ]
)

if delisted_pages:
    delisted_df = pd.concat(delisted_pages, ignore_index=True)
    delisted_df["ipoDate"] = pd.to_datetime(delisted_df["ipoDate"], errors="coerce")
    delisted_df["delistedDate"] = pd.to_datetime(
        delisted_df["delistedDate"],
        errors="coerce",
    )

    delisted_df = delisted_df[
        delisted_df["exchange"].isin(US_EXCHANGES)
        & delisted_df["ipoDate"].le(REFERENCE_MARKET_CAP_DATE)
        & delisted_df["delistedDate"].ge(REFERENCE_MARKET_CAP_DATE)
    ].copy()
    delisted_df = delisted_df.dropna(subset=["symbol", "companyName"])
    delisted_df = delisted_df[~delisted_df["symbol"].isin(EXCLUDED_TICKERS)]
    delisted_df = delisted_df.drop_duplicates(subset=["symbol"], keep="first")

    delisted_candidates = load_delisted_profiles_parallel(delisted_df)


# ---------------------------------------------------------------------------
# Combine Candidates Before Historical Reranking
# ---------------------------------------------------------------------------

candidate_frames = [
    frame for frame in [active_candidates, delisted_candidates] if not frame.empty
]
candidate_df = pd.concat(candidate_frames, ignore_index=True)
candidate_df = candidate_df.drop_duplicates(subset=["symbol"], keep="first")


# ---------------------------------------------------------------------------
# Replace Current Market Cap With Historical Market Cap At Sample Start
# ---------------------------------------------------------------------------

historical_candidates = load_historical_market_caps_parallel(candidate_df)
if historical_candidates.empty:
    raise ValueError("No historical market-cap observations were available.")


# ---------------------------------------------------------------------------
# Apply The Final Sector Ranking
# ---------------------------------------------------------------------------

# Keep one line per company name using the largest sample-start market cap.
selected_df = (
    historical_candidates.sort_values(by="marketCap", ascending=False)
    .drop_duplicates(subset=["companyName"], keep="first")
    .copy()
)

# Keep the final top companies per sector using the historical market cap.
selected_df = (
    selected_df.sort_values(by="marketCap", ascending=False)
    .groupby("sector", group_keys=False)
    .head(FINAL_COMPANIES_PER_SECTOR)
    .copy()
)

selected_df = selected_df[
    [
        "symbol",
        "companyName",
        "sector",
        "industry",
        "marketCap",
        "historicalMarketCapDate",
        "selectionReferenceDate",
        "universeSource",
    ]
]
selected_df = selected_df.rename(columns={"symbol": "Ticker"})


# ---------------------------------------------------------------------------
# Save Output
# ---------------------------------------------------------------------------

cfg.ENT.parent.mkdir(parents=True, exist_ok=True)
selected_df.to_csv(cfg.ENT, index=False)

print(f"Saved file: {cfg.ENT}")
print(
    "Historical company selection completed: "
    f"{selected_df['Ticker'].nunique()} ticker across "
    f"{selected_df['sector'].nunique()} sectors."
)


# %%
