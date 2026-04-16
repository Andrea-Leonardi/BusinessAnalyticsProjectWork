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
INCOME_STATEMENT_URL = f"{FMP_API_BASE_URL}/income-statement"
BALANCE_SHEET_URL = f"{FMP_API_BASE_URL}/balance-sheet-statement"
CASH_FLOW_STATEMENT_URL = f"{FMP_API_BASE_URL}/cash-flow-statement"
ENTERPRISE_VALUES_URL = f"{FMP_API_BASE_URL}/enterprise-values"


# ---------------------------------------------------------------------------
# Input And Output Paths
# ---------------------------------------------------------------------------

ENTERPRISES_PATH = cfg.ENT
RAW_OUTPUT_FILE = cfg.FMP_RAW_FINANCIALS
COMPANY_FINANCIAL_OUTPUT_DIR = cfg.SINGLE_COMPANY_FINANCIALS


# ---------------------------------------------------------------------------
# API Settings
# ---------------------------------------------------------------------------

FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
# Leave this empty to load all valid tickers from enterprises.csv.
SELECTED_TICKERS: list[str] = []

SELECTED_PERIOD = "quarter"
# Pull a longer quarterly history so trailing-twelve-month factors are already
# available when the weekly sample starts in early 2021.
STATEMENT_LIMIT = 60
REQUEST_TIMEOUT = 30
REQUEST_PAUSE_SECONDS = 0.4
MAX_REQUEST_ATTEMPTS = 5
RATE_LIMIT_WAIT_SECONDS = 5.0


# ---------------------------------------------------------------------------
# Merge And Export Settings
# ---------------------------------------------------------------------------

MERGE_KEY_CANDIDATES = [
    "requested_symbol",
    "symbol",
    "date",
    "fiscalYear",
    "period",
]

OUTPUT_METADATA_COLUMNS = [
    "requested_symbol",
    "symbol",
    "date",
    "fiscalYear",
    "period",
    "filingDate",
    "acceptedDate",
    "reportedCurrency",
    "cik",
]

EXPECTED_COMPANY_FINANCIAL_COLUMNS = ["WeekEndingFriday", "symbol"]
MIN_VALID_QUARTERLY_RELEASES = 4
KEY_FINANCIAL_SIGNAL_COLUMNS = [
    "MarketCap",
    "BookToMarket",
    "GrossProfitability",
    "OperatingMargin",
    "ROA",
]


# ---------------------------------------------------------------------------
# Requested Financial Fields
# ---------------------------------------------------------------------------

INCOME_FIELD_MAP = {
    "revenue": ["revenue"],
    "grossProfit": ["grossProfit"],
    "operatingIncome": ["operatingIncome"],
    "netIncome": ["netIncome", "bottomLineNetIncome"],
    "interestExpense": ["interestExpense"],
    "weightedAverageShsOut": ["weightedAverageShsOut"],
    "weightedAverageShsOutDil": ["weightedAverageShsOutDil"],
}

BALANCE_FIELD_MAP = {
    "totalAssets": ["totalAssets"],
    "totalStockholdersEquity": ["totalStockholdersEquity", "totalEquity"],
    "totalCurrentAssets": ["totalCurrentAssets"],
    "totalCurrentLiabilities": ["totalCurrentLiabilities"],
    "totalDebt": ["totalDebt"],
    "cashAndCashEquivalents": ["cashAndCashEquivalents"],
}

CASH_FLOW_FIELD_MAP = {
    "operatingCashFlow": ["operatingCashFlow"],
    "capitalExpenditure": ["capitalExpenditure"],
    "freeCashFlow": ["freeCashFlow"],
}

ENTERPRISE_VALUE_FIELD_MAP = {
    "marketCap": ["marketCapitalization", "marketCap"],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_target_tickers() -> list[str]:
    # Uso ticker manuali se esplicitati, altrimenti leggo enterprises.csv.
    if SELECTED_TICKERS:
        cleaned_tickers = [
            ticker.strip().upper()
            for ticker in SELECTED_TICKERS
            if ticker and ticker.strip()
        ]
        if not cleaned_tickers:
            raise ValueError("The ticker list is empty.")
        return list(dict.fromkeys(cleaned_tickers))

    if not ENTERPRISES_PATH.exists():
        raise FileNotFoundError(f"Company file not found: {ENTERPRISES_PATH}")

    companies = pd.read_csv(ENTERPRISES_PATH)
    if "Ticker" not in companies.columns:
        raise KeyError("The 'Ticker' column does not exist in enterprises.csv")

    tickers = companies["Ticker"].dropna().astype(str).str.strip()
    tickers = tickers[tickers.ne("")].drop_duplicates().tolist()
    return tickers


def get_company_financial_output_path(ticker: str) -> Path:
    return COMPANY_FINANCIAL_OUTPUT_DIR / f"{ticker}Financials.csv"


def company_financial_file_is_usable(file_path: Path) -> bool:
    # Considero valido solo un file finanziario aziendale leggibile e non vuoto.
    if not file_path.exists():
        return False

    try:
        company_df = pd.read_csv(file_path)
    except Exception:
        return False

    if company_df.empty:
        return False

    missing_columns = [
        column for column in EXPECTED_COMPANY_FINANCIAL_COLUMNS if column not in company_df.columns
    ]
    if missing_columns:
        return False

    if "QuarterlyReleased" in company_df.columns:
        quarterly_releases = pd.to_numeric(
            company_df["QuarterlyReleased"],
            errors="coerce",
        ).fillna(0).sum()
        if quarterly_releases < MIN_VALID_QUARTERLY_RELEASES:
            return False

    available_signal_columns = [
        column for column in KEY_FINANCIAL_SIGNAL_COLUMNS if column in company_df.columns
    ]
    if available_signal_columns and company_df[available_signal_columns].isna().all().all():
        return False

    return True


def parse_datetime_mixed(series: pd.Series) -> pd.Series:
    # Gestisco colonne con timestamp misti, ad esempio date-only e datetime.
    parsed = pd.to_datetime(series, errors="coerce")
    remaining_mask = series.notna() & parsed.isna()
    if not remaining_mask.any():
        return parsed

    try:
        reparsed = pd.to_datetime(
            series.loc[remaining_mask],
            errors="coerce",
            format="mixed",
        )
    except TypeError:
        reparsed = series.loc[remaining_mask].apply(
            lambda value: pd.to_datetime(value, errors="coerce")
        )

    parsed.loc[remaining_mask] = reparsed
    return parsed


def load_existing_raw_financials(valid_ticker_set: set[str]) -> pd.DataFrame:
    # Se esiste gia un raw aggregato, lo riuso come base e tengo solo il
    # perimetro ticker corrente.
    if not RAW_OUTPUT_FILE.exists():
        return pd.DataFrame()

    existing_raw_df = pd.read_csv(RAW_OUTPUT_FILE)
    if "requested_symbol" not in existing_raw_df.columns:
        return pd.DataFrame()

    existing_raw_df["requested_symbol"] = (
        existing_raw_df["requested_symbol"].astype(str).str.strip().str.upper()
    )
    existing_raw_df = existing_raw_df[existing_raw_df["requested_symbol"].isin(valid_ticker_set)].copy()
    return existing_raw_df


def load_company_names() -> dict[str, str]:
    # Recupero i nomi societari quando disponibili in enterprises.csv.
    if not ENTERPRISES_PATH.exists():
        return {}

    companies = pd.read_csv(ENTERPRISES_PATH)
    if "Ticker" not in companies.columns:
        return {}

    companies = companies.dropna(subset=["Ticker"]).copy()
    companies["Ticker"] = companies["Ticker"].astype(str).str.strip()
    companies = companies[companies["Ticker"].ne("")]

    if "Company_name" in companies.columns:
        return companies.set_index("Ticker")["Company_name"].to_dict()
    if "companyName" in companies.columns:
        return companies.set_index("Ticker")["companyName"].to_dict()

    return {}


def download_statement_payload(
    session: requests.Session,
    ticker: str,
    statement_name: str,
    endpoint_url: str,
):
    # Scarico una singola famiglia di statement con retry e gestione rate limit.
    payload = None

    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        response = session.get(
            endpoint_url,
            params={
                "symbol": ticker,
                "period": SELECTED_PERIOD,
                "limit": STATEMENT_LIMIT,
                "apikey": FMP_API_KEY,
            },
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
                f"Rate limit for {ticker} on {statement_name} "
                f"(attempt {attempt}/{MAX_REQUEST_ATTEMPTS}). "
                f"Waiting {wait_seconds:.1f} seconds before retrying..."
            )
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        payload = response.json()
        time.sleep(REQUEST_PAUSE_SECONDS)
        break

    if payload is None:
        raise requests.HTTPError(
            f"429 Too Many Requests for {ticker} on {statement_name} "
            f"after {MAX_REQUEST_ATTEMPTS} attempts."
        )

    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict) and isinstance(payload.get("historical"), list):
        records = payload["historical"]
        if payload.get("symbol"):
            records = [{**record, "symbol": payload["symbol"]} for record in records]
    else:
        raise ValueError(f"Unexpected FMP response for {ticker}: {payload}")

    if not records:
        return pd.DataFrame()

    statement_df = pd.DataFrame(records)
    statement_df.insert(0, "requested_symbol", ticker)

    if "symbol" not in statement_df.columns:
        statement_df.insert(1, "symbol", ticker)

    for date_col in ("date", "filingDate", "acceptedDate", "fillingDate"):
        if date_col in statement_df.columns:
            statement_df[date_col] = parse_datetime_mixed(statement_df[date_col])

    if "date" in statement_df.columns:
        statement_df = statement_df.sort_values("date", ascending=False).reset_index(drop=True)

    return statement_df


def build_company_raw_financials(
    ticker: str,
    company_names: dict[str, str],
    session: requests.Session,
) -> pd.DataFrame:
    # Scarico e unisco i quattro statement raw di FMP per un singolo ticker.
    statement_frames = {}

    for statement_name, endpoint_url in {
        "income": INCOME_STATEMENT_URL,
        "balance": BALANCE_SHEET_URL,
        "cash_flow": CASH_FLOW_STATEMENT_URL,
        "enterprise_values": ENTERPRISE_VALUES_URL,
    }.items():
        statement_frames[statement_name] = download_statement_payload(
            session=session,
            ticker=ticker,
            statement_name=statement_name,
            endpoint_url=endpoint_url,
        )

    income_df = statement_frames["income"]
    balance_df = statement_frames["balance"]
    cash_flow_df = statement_frames["cash_flow"]
    enterprise_values_df = statement_frames["enterprise_values"]

    if income_df.empty or balance_df.empty or cash_flow_df.empty or enterprise_values_df.empty:
        return pd.DataFrame()

    income_balance_merge_keys = [
        column for column in MERGE_KEY_CANDIDATES if column in income_df.columns and column in balance_df.columns
    ]
    income_cash_flow_merge_keys = [
        column for column in MERGE_KEY_CANDIDATES if column in income_df.columns and column in cash_flow_df.columns
    ]
    income_enterprise_merge_keys = [
        column
        for column in MERGE_KEY_CANDIDATES
        if column in income_df.columns and column in enterprise_values_df.columns
    ]

    if not income_balance_merge_keys or not income_cash_flow_merge_keys or not income_enterprise_merge_keys:
        return pd.DataFrame()

    for statement_df, merge_keys in (
        (income_df, income_balance_merge_keys),
        (balance_df, income_balance_merge_keys),
        (cash_flow_df, income_cash_flow_merge_keys),
        (enterprise_values_df, income_enterprise_merge_keys),
    ):
        sort_columns = [
            column
            for column in ("acceptedDate", "filingDate", "fillingDate", "date")
            if column in statement_df.columns
        ]
        if sort_columns:
            statement_df.sort_values(sort_columns, ascending=False, inplace=True)
        statement_df.drop_duplicates(subset=merge_keys, keep="first", inplace=True)

    income_df = income_df.reset_index(drop=True)
    balance_df = balance_df.reset_index(drop=True)
    cash_flow_df = cash_flow_df.reset_index(drop=True)
    enterprise_values_df = enterprise_values_df.reset_index(drop=True)

    income_base_columns = [
        column for column in OUTPUT_METADATA_COLUMNS if column in income_df.columns
    ]
    income_selected = income_df[income_base_columns].copy()
    for output_name, candidate_columns in INCOME_FIELD_MAP.items():
        primary_column = candidate_columns[0]
        if primary_column in income_df.columns:
            income_selected[output_name] = income_df[primary_column]
            continue

        fallback_column = next((column for column in candidate_columns[1:] if column in income_df.columns), None)
        income_selected[output_name] = income_df[fallback_column] if fallback_column is not None else pd.NA

    balance_selected = balance_df[income_balance_merge_keys].copy()
    for output_name, candidate_columns in BALANCE_FIELD_MAP.items():
        primary_column = candidate_columns[0]
        if primary_column in balance_df.columns:
            balance_selected[output_name] = balance_df[primary_column]
            continue

        fallback_column = next((column for column in candidate_columns[1:] if column in balance_df.columns), None)
        balance_selected[output_name] = balance_df[fallback_column] if fallback_column is not None else pd.NA

    cash_flow_selected = cash_flow_df[income_cash_flow_merge_keys].copy()
    for output_name, candidate_columns in CASH_FLOW_FIELD_MAP.items():
        primary_column = candidate_columns[0]
        if primary_column in cash_flow_df.columns:
            cash_flow_selected[output_name] = cash_flow_df[primary_column]
            continue

        fallback_column = next((column for column in candidate_columns[1:] if column in cash_flow_df.columns), None)
        cash_flow_selected[output_name] = cash_flow_df[fallback_column] if fallback_column is not None else pd.NA

    enterprise_values_selected = enterprise_values_df[income_enterprise_merge_keys].copy()
    for output_name, candidate_columns in ENTERPRISE_VALUE_FIELD_MAP.items():
        primary_column = candidate_columns[0]
        if primary_column in enterprise_values_df.columns:
            enterprise_values_selected[output_name] = enterprise_values_df[primary_column]
            continue

        fallback_column = next(
            (column for column in candidate_columns[1:] if column in enterprise_values_df.columns),
            None,
        )
        enterprise_values_selected[output_name] = (
            enterprise_values_df[fallback_column] if fallback_column is not None else pd.NA
        )

    company_df = income_selected.merge(
        balance_selected,
        on=income_balance_merge_keys,
        how="inner",
    )
    company_df = company_df.merge(
        cash_flow_selected,
        on=income_cash_flow_merge_keys,
        how="inner",
    )
    company_df = company_df.merge(
        enterprise_values_selected,
        on=income_enterprise_merge_keys,
        how="left",
    )

    ordered_columns = (
        [column for column in OUTPUT_METADATA_COLUMNS if column in company_df.columns]
        + list(INCOME_FIELD_MAP.keys())
        + list(BALANCE_FIELD_MAP.keys())
        + list(CASH_FLOW_FIELD_MAP.keys())
        + list(ENTERPRISE_VALUE_FIELD_MAP.keys())
    )
    company_df = company_df[ordered_columns]

    company_name = company_names.get(ticker)
    if company_name:
        company_df.insert(1, "company_name", company_name)

    return company_df


def main() -> None:
    # -----------------------------------------------------------------------
    # Validate Settings
    # -----------------------------------------------------------------------

    if not FMP_API_KEY or FMP_API_KEY == "INSERISCI_LA_TUA_API_KEY":
        raise EnvironmentError(
            "Set your FMP_API_KEY directly in this file before running the script."
        )

    if SELECTED_PERIOD not in {"annual", "quarter"}:
        raise ValueError("SELECTED_PERIOD must be either 'annual' or 'quarter'.")

    # -----------------------------------------------------------------------
    # Load Company List And Existing Files
    # -----------------------------------------------------------------------

    tickers = load_target_tickers()
    valid_ticker_set = set(tickers)
    company_names = load_company_names()

    COMPANY_FINANCIAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    existing_raw_df = load_existing_raw_financials(valid_ticker_set)

    tickers_to_download = []
    skipped_existing_tickers = []
    for ticker in tickers:
        company_financial_path = get_company_financial_output_path(ticker)
        if company_financial_file_is_usable(company_financial_path):
            skipped_existing_tickers.append(ticker)
            continue

        tickers_to_download.append(ticker)

    # -----------------------------------------------------------------------
    # Download Missing Raw Financial Data
    # -----------------------------------------------------------------------

    downloaded_frames = []
    failed_tickers = []

    with requests.Session() as session:
        for ticker in tickers_to_download:
            print(f"Downloading raw financial statements for {ticker}...")

            try:
                company_df = build_company_raw_financials(
                    ticker=ticker,
                    company_names=company_names,
                    session=session,
                )
            except requests.HTTPError as exc:
                print(f"HTTP error for {ticker}: {exc}")
                failed_tickers.append(ticker)
                continue
            except requests.RequestException as exc:
                print(f"Network error for {ticker}: {exc}")
                failed_tickers.append(ticker)
                continue
            except ValueError as exc:
                print(f"Data error for {ticker}: {exc}")
                failed_tickers.append(ticker)
                continue

            if company_df.empty:
                print(f"Incomplete or empty raw financial data for {ticker}")
                failed_tickers.append(ticker)
                continue

            downloaded_frames.append(company_df)

    # -----------------------------------------------------------------------
    # Save Final Raw Output
    # -----------------------------------------------------------------------

    downloaded_ticker_set = {
        frame["requested_symbol"].iloc[0]
        for frame in downloaded_frames
        if not frame.empty and "requested_symbol" in frame.columns
    }

    preserved_existing_raw_df = pd.DataFrame()
    if not existing_raw_df.empty:
        preserved_existing_raw_df = existing_raw_df[
            ~existing_raw_df["requested_symbol"].isin(downloaded_ticker_set)
        ].copy()

    combined_frames = []
    if not preserved_existing_raw_df.empty:
        combined_frames.append(preserved_existing_raw_df)
    combined_frames.extend(downloaded_frames)

    if not combined_frames:
        print("No new raw financial downloads were required and no raw cache was available.")
        return

    combined_df = pd.concat(combined_frames, ignore_index=True, sort=False)

    if {"requested_symbol", "date"}.issubset(combined_df.columns):
        combined_df = combined_df.sort_values(
            ["requested_symbol", "date"],
            ascending=[True, False],
        ).reset_index(drop=True)
    elif "requested_symbol" in combined_df.columns:
        combined_df = combined_df.sort_values("requested_symbol").reset_index(drop=True)

    combined_df.to_csv(RAW_OUTPUT_FILE, index=False)
    print(
        "Raw financial gathering completed:",
        {
            "valid_tickers": len(tickers),
            "downloaded_tickers": sorted(downloaded_ticker_set),
            "skipped_existing_financial_files": len(skipped_existing_tickers),
            "failed_tickers": failed_tickers,
            "raw_output": str(RAW_OUTPUT_FILE),
            "raw_rows": len(combined_df),
            "raw_tickers": combined_df["requested_symbol"].nunique() if "requested_symbol" in combined_df.columns else 0,
        },
    )


if __name__ == "__main__":
    main()


# %%
