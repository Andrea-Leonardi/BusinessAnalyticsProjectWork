#%%
import time
from pathlib import Path

import pandas as pd
import requests
import config as cfg


INCOME_STATEMENT_URL = "https://financialmodelingprep.com/stable/income-statement"
BALANCE_SHEET_URL = "https://financialmodelingprep.com/stable/balance-sheet-statement"

ENTERPRISES_PATH = cfg.ENT
OUTPUT_FILE = cfg.FMP_RAW_FINANCIALS

FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
# Leave this empty to load the first ENTERPRISE_ROW_LIMIT companies from enterprises.csv.
SELECTED_TICKERS: list[str] = []
SELECTED_PERIOD = "quarter"
STATEMENT_LIMIT = 20
ENTERPRISE_ROW_LIMIT = 10
REQUEST_TIMEOUT = 30
REQUEST_PAUSE_SECONDS = 0.25
MAX_SUBSCRIPTION_LIMIT = 20

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

INCOME_FIELD_MAP = {
    "revenue": ["revenue"],
    "netIncome": ["netIncome", "bottomLineNetIncome"],
    "operatingIncome": ["operatingIncome"],
    "ebitda": ["ebitda"],
}

BALANCE_FIELD_MAP = {
    "totalStockholdersEquity": ["totalStockholdersEquity", "totalEquity"],
    "totalAssets": ["totalAssets"],
    "totalDebt": ["totalDebt"],
    "totalCurrentAssets": ["totalCurrentAssets", "currentAssets"],
    "totalCurrentLiabilities": ["totalCurrentLiabilities", "currentLiabilities"],
}


def load_companies(limit: int | None = ENTERPRISE_ROW_LIMIT) -> pd.DataFrame:
    if not ENTERPRISES_PATH.exists():
        raise FileNotFoundError(f"Company file not found: {ENTERPRISES_PATH}")

    companies = pd.read_csv(ENTERPRISES_PATH)
    if "Ticker" not in companies.columns:
        raise KeyError("The 'Ticker' column does not exist in enterprises.csv")

    companies = companies.dropna(subset=["Ticker"]).copy()
    companies["Ticker"] = companies["Ticker"].astype(str).str.strip()
    companies = companies[companies["Ticker"] != ""]

    if limit is not None:
        companies = companies.head(limit).copy()

    return companies


def build_companies_from_tickers(tickers: list[str]) -> pd.DataFrame:
    cleaned_tickers = [ticker.strip().upper() for ticker in tickers if ticker and ticker.strip()]
    if not cleaned_tickers:
        raise ValueError("The ticker list is empty.")

    return pd.DataFrame({"Ticker": cleaned_tickers})


def validate_settings() -> None:
    if SELECTED_PERIOD not in {"annual", "quarter"}:
        raise ValueError("SELECTED_PERIOD must be either 'annual' or 'quarter'.")

    if STATEMENT_LIMIT < 0 or STATEMENT_LIMIT > MAX_SUBSCRIPTION_LIMIT:
        raise ValueError(
            f"STATEMENT_LIMIT must be between 0 and {MAX_SUBSCRIPTION_LIMIT} "
            "for the current FMP subscription."
        )

    if ENTERPRISE_ROW_LIMIT is not None and ENTERPRISE_ROW_LIMIT < 0:
        raise ValueError("ENTERPRISE_ROW_LIMIT must be greater than or equal to 0.")


def fetch_statement_data(
    endpoint_url: str,
    symbol: str,
    api_key: str,
    period: str,
    limit: int,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    params = {
        "symbol": symbol,
        "period": period,
        "limit": limit,
        "apikey": api_key,
    }

    http = session or requests.Session()
    response = http.get(endpoint_url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, list):
        raise ValueError(f"Unexpected FMP response for {symbol}: {payload}")

    if not payload:
        return pd.DataFrame()

    statement_df = pd.DataFrame(payload)
    statement_df.insert(0, "requested_symbol", symbol)

    for date_col in ("date", "filingDate", "acceptedDate", "fillingDate"):
        if date_col in statement_df.columns:
            statement_df[date_col] = pd.to_datetime(statement_df[date_col], errors="coerce")

    if "date" in statement_df.columns:
        statement_df = statement_df.sort_values("date", ascending=False).reset_index(drop=True)

    return statement_df


def fetch_income_statement(
    symbol: str,
    api_key: str,
    period: str = SELECTED_PERIOD,
    limit: int = STATEMENT_LIMIT,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    return fetch_statement_data(
        endpoint_url=INCOME_STATEMENT_URL,
        symbol=symbol,
        api_key=api_key,
        period=period,
        limit=limit,
        session=session,
    )


def fetch_balance_sheet_statement(
    symbol: str,
    api_key: str,
    period: str = SELECTED_PERIOD,
    limit: int = STATEMENT_LIMIT,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    return fetch_statement_data(
        endpoint_url=BALANCE_SHEET_URL,
        symbol=symbol,
        api_key=api_key,
        period=period,
        limit=limit,
        session=session,
    )


def select_requested_fields(
    statement_df: pd.DataFrame,
    field_map: dict[str, list[str]],
    base_columns: list[str],
) -> pd.DataFrame:
    selected_df = statement_df[base_columns].copy()

    for output_name, candidate_columns in field_map.items():
        source_column = next(
            (column for column in candidate_columns if column in statement_df.columns),
            None,
        )
        selected_df[output_name] = (
            statement_df[source_column] if source_column is not None else pd.NA
        )

    return selected_df


def deduplicate_statement_rows(statement_df: pd.DataFrame, key_columns: list[str]) -> pd.DataFrame:
    if statement_df.empty or not key_columns:
        return statement_df

    sort_columns = [
        column
        for column in ("acceptedDate", "filingDate", "fillingDate", "date")
        if column in statement_df.columns
    ]
    if sort_columns:
        statement_df = statement_df.sort_values(sort_columns, ascending=False)

    return statement_df.drop_duplicates(subset=key_columns, keep="first").reset_index(drop=True)


def merge_financial_statements(
    income_df: pd.DataFrame,
    balance_df: pd.DataFrame,
) -> pd.DataFrame:
    merge_keys = [
        column
        for column in MERGE_KEY_CANDIDATES
        if column in income_df.columns and column in balance_df.columns
    ]
    if not merge_keys:
        raise ValueError("No common keys were found to merge income and balance data.")

    income_df = deduplicate_statement_rows(income_df, merge_keys)
    balance_df = deduplicate_statement_rows(balance_df, merge_keys)

    income_base_columns = [
        column for column in OUTPUT_METADATA_COLUMNS if column in income_df.columns
    ]
    income_selected = select_requested_fields(income_df, INCOME_FIELD_MAP, income_base_columns)
    balance_selected = select_requested_fields(balance_df, BALANCE_FIELD_MAP, merge_keys)

    merged_df = income_selected.merge(balance_selected, on=merge_keys, how="inner")
    metadata_columns = [column for column in OUTPUT_METADATA_COLUMNS if column in merged_df.columns]
    ordered_columns = metadata_columns + list(INCOME_FIELD_MAP.keys()) + list(BALANCE_FIELD_MAP.keys())
    return merged_df[ordered_columns]


def download_raw_financial_statements(
    companies: pd.DataFrame,
    api_key: str,
    period: str = SELECTED_PERIOD,
    limit: int = STATEMENT_LIMIT,
    output_file: Path = OUTPUT_FILE,
    pause_seconds: float = REQUEST_PAUSE_SECONDS,
) -> pd.DataFrame:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    combined_frames: list[pd.DataFrame] = []
    company_names = {}
    if "Company_name" in companies.columns:
        company_names = companies.set_index("Ticker")["Company_name"].to_dict()

    tickers = companies["Ticker"].dropna().astype(str).str.strip().unique()

    with requests.Session() as session:
        for ticker in tickers:
            print(f"Downloading raw financial statements for {ticker}...")
            try:
                income_df = fetch_income_statement(
                    symbol=ticker,
                    api_key=api_key,
                    period=period,
                    limit=limit,
                    session=session,
                )
                balance_df = fetch_balance_sheet_statement(
                    symbol=ticker,
                    api_key=api_key,
                    period=period,
                    limit=limit,
                    session=session,
                )
            except requests.HTTPError as exc:
                print(f"HTTP error for {ticker}: {exc}")
                continue
            except requests.RequestException as exc:
                print(f"Network error for {ticker}: {exc}")
                continue
            except ValueError as exc:
                print(f"Data error for {ticker}: {exc}")
                continue

            if income_df.empty:
                print(f"No income statement data available for {ticker}")
                continue

            if balance_df.empty:
                print(f"No balance sheet data available for {ticker}")
                continue

            company_df = merge_financial_statements(income_df, balance_df)
            company_name = company_names.get(ticker)
            if company_name:
                company_df.insert(1, "company_name", company_name)

            combined_frames.append(company_df)
            time.sleep(pause_seconds)

    if not combined_frames:
        return pd.DataFrame()

    combined_df = pd.concat(combined_frames, ignore_index=True)
    if {"requested_symbol", "date"}.issubset(combined_df.columns):
        combined_df = combined_df.sort_values(
            ["requested_symbol", "date"],
            ascending=[True, False],
        ).reset_index(drop=True)
    elif "requested_symbol" in combined_df.columns:
        combined_df = combined_df.sort_values("requested_symbol").reset_index(drop=True)

    combined_df.to_csv(output_file, index=False)
    print(f"Saved raw file: {output_file}")
    return combined_df


def main() -> None:
    if not FMP_API_KEY or FMP_API_KEY == "INSERISCI_LA_TUA_API_KEY":
        raise EnvironmentError(
            "Set your FMP_API_KEY directly in this file before running the script."
        )

    validate_settings()

    if SELECTED_TICKERS:
        companies = build_companies_from_tickers(SELECTED_TICKERS)
    else:
        companies = load_companies(limit=ENTERPRISE_ROW_LIMIT)

    combined_df = download_raw_financial_statements(
        companies=companies,
        api_key=FMP_API_KEY,
        period=SELECTED_PERIOD,
        limit=STATEMENT_LIMIT,
        output_file=OUTPUT_FILE,
    )

    if combined_df.empty:
        print("Download completed, but no raw records were saved.")
        return

    print(
        "Raw download completed: "
        f"{combined_df['requested_symbol'].nunique()} ticker, "
        f"{len(combined_df)} total rows."
    )


if __name__ == "__main__":
    main()


# %%
