#%%
import sys
import time
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
REFERENCE_MARKET_CAP_DATE = pd.Timestamp("2021-01-04")
MARKET_CAP_LOOKBACK_DATE = "2020-12-28"
MARKET_CAP_LOOKFORWARD_DATE = "2021-01-15"

# Use a broad active candidate set before reranking on historical market cap.
ACTIVE_CANDIDATE_BUFFER_PER_SECTOR = 100
FINAL_COMPANIES_PER_SECTOR = 10

DELISTED_PAGE_SIZE = 100
MAX_DELISTED_PAGES = 100
REQUEST_TIMEOUT = 30
REQUEST_PAUSE_SECONDS = 0.25
MAX_REQUEST_ATTEMPTS = 5
RATE_LIMIT_WAIT_SECONDS = 5.0

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
}


def fetch_json(
    session: requests.Session,
    url: str,
    params: dict[str, object],
    context: str,
) -> list[dict] | dict:
    """Download one FMP payload with a simple retry loop."""
    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        response = session.get(
            url,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

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
        payload = response.json()
        time.sleep(REQUEST_PAUSE_SECONDS)
        return payload

    raise requests.HTTPError(
        f"429 Too Many Requests for {context} after {MAX_REQUEST_ATTEMPTS} attempts."
    )


def build_active_candidate_universe(session: requests.Session) -> pd.DataFrame:
    """Load a broad current US universe before reranking it historically."""
    active_frames: list[pd.DataFrame] = []

    for exchange in US_EXCHANGES:
        payload = fetch_json(
            session=session,
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
        return pd.DataFrame()

    active_df = pd.concat(active_frames, ignore_index=True)
    active_df = active_df.dropna(subset=["symbol", "companyName", "sector"]).copy()
    active_df = active_df[active_df["symbol"].astype(str).str.strip() != ""]
    active_df = active_df[~active_df["symbol"].isin(EXCLUDED_TICKERS)]
    active_df = active_df.sort_values("marketCap", ascending=False)

    # Oversample each sector from the active universe before replacing the
    # ranking metric with historical market cap at the sample start.
    active_df = (
        active_df.groupby("sector", group_keys=False)
        .head(ACTIVE_CANDIDATE_BUFFER_PER_SECTOR)
        .copy()
    )
    active_df["universeSource"] = "active_screener"

    return active_df[
        [
            "symbol",
            "companyName",
            "sector",
            "industry",
            "exchangeShortName",
            "universeSource",
        ]
    ]


def build_delisted_candidate_universe(session: requests.Session) -> pd.DataFrame:
    """Augment the universe with US tickers that delisted after the sample start."""
    delisted_pages: list[pd.DataFrame] = []

    for page in range(MAX_DELISTED_PAGES):
        try:
            payload = fetch_json(
                session=session,
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

    if not delisted_pages:
        return pd.DataFrame()

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

    delisted_candidates: list[dict[str, object]] = []
    total_candidates = len(delisted_df)

    for index, row in enumerate(delisted_df.itertuples(index=False), start=1):
        print(
            f"Loading delisted profile for {row.symbol} "
            f"({index}/{total_candidates})..."
        )

        payload = fetch_json(
            session=session,
            url=PROFILE_URL,
            params={
                "apikey": FMP_API_KEY,
                "symbol": row.symbol,
            },
            context=f"profile {row.symbol}",
        )

        if not isinstance(payload, list) or not payload:
            continue

        profile = payload[0]
        if profile.get("isEtf") or profile.get("isFund"):
            continue

        sector = profile.get("sector")
        if not sector:
            continue

        delisted_candidates.append(
            {
                "symbol": row.symbol,
                "companyName": profile.get("companyName") or row.companyName,
                "sector": sector,
                "industry": profile.get("industry"),
                "exchangeShortName": profile.get("exchange") or row.exchange,
                "universeSource": "post_2021_delisted",
            }
        )

    return pd.DataFrame(delisted_candidates)


def select_reference_market_cap(
    market_cap_payload: list[dict],
) -> tuple[object, object]:
    """Pick the closest market-cap observation around the reference date."""
    if not market_cap_payload:
        return pd.NA, pd.NaT

    market_cap_df = pd.DataFrame(market_cap_payload)
    if market_cap_df.empty or "date" not in market_cap_df.columns:
        return pd.NA, pd.NaT

    market_cap_df["date"] = pd.to_datetime(market_cap_df["date"], errors="coerce")
    market_cap_df["marketCap"] = pd.to_numeric(
        market_cap_df["marketCap"],
        errors="coerce",
    )
    market_cap_df = market_cap_df.dropna(subset=["date", "marketCap"]).copy()
    if market_cap_df.empty:
        return pd.NA, pd.NaT

    on_or_after_df = market_cap_df[
        market_cap_df["date"] >= REFERENCE_MARKET_CAP_DATE
    ].sort_values("date", ascending=True)
    if not on_or_after_df.empty:
        selected_row = on_or_after_df.iloc[0]
        return float(selected_row["marketCap"]), selected_row["date"]

    before_df = market_cap_df[
        market_cap_df["date"] < REFERENCE_MARKET_CAP_DATE
    ].sort_values("date", ascending=False)
    if before_df.empty:
        return pd.NA, pd.NaT

    selected_row = before_df.iloc[0]
    return float(selected_row["marketCap"]), selected_row["date"]


def attach_historical_market_caps(
    session: requests.Session,
    candidate_df: pd.DataFrame,
) -> pd.DataFrame:
    """Replace current market cap with the closest observation at sample start."""
    enriched_rows: list[dict[str, object]] = []
    total_candidates = len(candidate_df)

    for index, row in enumerate(candidate_df.itertuples(index=False), start=1):
        print(
            f"Loading historical market cap for {row.symbol} "
            f"({index}/{total_candidates})..."
        )

        payload = fetch_json(
            session=session,
            url=HISTORICAL_MARKET_CAP_URL,
            params={
                "apikey": FMP_API_KEY,
                "symbol": row.symbol,
                "from": MARKET_CAP_LOOKBACK_DATE,
                "to": MARKET_CAP_LOOKFORWARD_DATE,
            },
            context=f"historical market cap {row.symbol}",
        )

        historical_market_cap, historical_market_cap_date = select_reference_market_cap(
            payload if isinstance(payload, list) else payload.get("historical", [])
        )
        if pd.isna(historical_market_cap):
            continue

        enriched_rows.append(
            {
                "symbol": row.symbol,
                "companyName": row.companyName,
                "sector": row.sector,
                "industry": row.industry,
                "exchangeShortName": row.exchangeShortName,
                "marketCap": historical_market_cap,
                "historicalMarketCapDate": historical_market_cap_date.strftime(
                    "%Y-%m-%d"
                ),
                "selectionReferenceDate": REFERENCE_MARKET_CAP_DATE.strftime("%Y-%m-%d"),
                "universeSource": row.universeSource,
            }
        )

    return pd.DataFrame(enriched_rows)


def main() -> None:
    # -----------------------------------------------------------------------
    # Build Historical Ranking Universe
    # -----------------------------------------------------------------------

    with requests.Session() as session:
        active_candidates = build_active_candidate_universe(session)
        delisted_candidates = build_delisted_candidate_universe(session)

        candidate_frames = [
            frame for frame in [active_candidates, delisted_candidates] if not frame.empty
        ]
        if not candidate_frames:
            raise ValueError("No company candidates were downloaded from FMP.")

        candidate_df = pd.concat(candidate_frames, ignore_index=True)
        candidate_df = candidate_df.drop_duplicates(subset=["symbol"], keep="first")

        historical_candidates = attach_historical_market_caps(session, candidate_df)

    if historical_candidates.empty:
        raise ValueError("No historical market-cap observations were available.")

    # -----------------------------------------------------------------------
    # Apply Final Selection Rules
    # -----------------------------------------------------------------------

    # Keep one line per company name using the largest sample-start market cap.
    selected_df = (
        historical_candidates.sort_values(by="marketCap", ascending=False)
        .drop_duplicates(subset=["companyName"], keep="first")
        .copy()
    )

    # Keep the largest companies inside each sector using the 2021 reference
    # market cap instead of today's market cap.
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

    # -----------------------------------------------------------------------
    # Save Output
    # -----------------------------------------------------------------------

    cfg.ENT.parent.mkdir(parents=True, exist_ok=True)
    selected_df.to_csv(cfg.ENT, index=False)
    print(f"Saved file: {cfg.ENT}")
    print(
        "Historical company selection completed: "
        f"{selected_df['Ticker'].nunique()} ticker across "
        f"{selected_df['sector'].nunique()} sectors."
    )


if __name__ == "__main__":
    main()


# %%
