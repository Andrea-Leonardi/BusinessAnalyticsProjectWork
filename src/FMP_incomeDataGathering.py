#%%
import time
from pathlib import Path

import pandas as pd
import requests
import config as cfg


# Base endpoint for the Financial Modeling Prep income statement API.
INCOME_STATEMENT_URL = "https://financialmodelingprep.com/stable/income-statement"
BALANCE_SHEET_URL = "https://financialmodelingprep.com/stable/balance-sheet-statement"

# Project paths are centralized in config.py to avoid duplicating path logic here.
ENTERPRISES_PATH = cfg.ENT
OUTPUT_DIR = cfg.FMP

# Main script settings.
# Fill in your FMP API key here before running the script.
FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"

# If this list is not empty, the script downloads data only for these tickers.
# If it is empty, the script loads companies from the enterprises CSV file.
SELECTED_TICKERS: list[str] = ["GS", "AAPL", "MSFT"]

# Financial statement frequency requested from FMP.
SELECTED_PERIOD = "quarter"

# Maximum number of statements requested per company.
STATEMENT_LIMIT = 5

# Number of companies to load from the project CSV when SELECTED_TICKERS is empty.
SELECTED_COMPANY_LIMIT = 10

# HTTP timeout for each API request.
REQUEST_TIMEOUT = 30

# Short pause between tickers to avoid hitting the API too aggressively.
REQUEST_PAUSE_SECONDS = 0.25

# Current subscription constraint for the "limit" parameter.
MAX_SUBSCRIPTION_LIMIT = 5

# Minimal keys used to align income statement rows with balance sheet rows.
# Using a smaller set is more robust than matching on every metadata field,
# because some non-essential fields may differ slightly between endpoints.
MERGE_KEY_CANDIDATES = [
    "requested_symbol",
    "symbol",
    "date",
    "fiscalYear",
    "period",
]

# Metadata columns preserved in the final output when they are available.
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

# Requested output fields using FMP variable names.
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


def load_companies(limit: int | None = SELECTED_COMPANY_LIMIT) -> pd.DataFrame:
    """
    Load the company list from the project CSV file.

    The function reads the file defined in config.py, checks that the
    required 'Ticker' column exists, removes missing or empty tickers,
    and optionally keeps only the first N companies.
    """
    if not ENTERPRISES_PATH.exists():
        raise FileNotFoundError(f"Company file not found: {ENTERPRISES_PATH}")

    companies = pd.read_csv(ENTERPRISES_PATH)
    if "Ticker" not in companies.columns:
        raise KeyError("The 'Ticker' column does not exist in enterprises.csv")

    # Keep only valid, non-empty ticker values.
    companies = companies.dropna(subset=["Ticker"]).copy()
    companies["Ticker"] = companies["Ticker"].astype(str).str.strip()
    companies = companies[companies["Ticker"] != ""]

    # Limit the number of companies when requested.
    if limit is not None:
        companies = companies.head(limit).copy()

    return companies


def build_companies_from_tickers(tickers: list[str]) -> pd.DataFrame:
    """
    Build a minimal company DataFrame starting from a manual ticker list.

    This is useful when you want to test the API on a small custom set
    of symbols instead of reading the project CSV file.
    """
    # Normalize user-provided tickers by trimming spaces and converting to uppercase.
    cleaned_tickers = [ticker.strip().upper() for ticker in tickers if ticker and ticker.strip()]
    if not cleaned_tickers:
        raise ValueError("The ticker list is empty.")

    return pd.DataFrame({"Ticker": cleaned_tickers})


def validate_settings() -> None:
    """
    Validate script settings before making API calls.

    In particular, this prevents requests that exceed the limit allowed
    by the current FMP subscription plan.
    """
    if SELECTED_PERIOD not in {"annual", "quarter"}:
        raise ValueError("SELECTED_PERIOD must be either 'annual' or 'quarter'.")

    if STATEMENT_LIMIT < 0 or STATEMENT_LIMIT > MAX_SUBSCRIPTION_LIMIT:
        raise ValueError(
            f"STATEMENT_LIMIT must be between 0 and {MAX_SUBSCRIPTION_LIMIT} "
            "for the current FMP subscription."
        )

    if SELECTED_COMPANY_LIMIT < 0:
        raise ValueError("SELECTED_COMPANY_LIMIT must be greater than or equal to 0.")


def fetch_income_statement(
    symbol: str,
    api_key: str,
    period: str = SELECTED_PERIOD,
    limit: int = STATEMENT_LIMIT,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """
    Download historical income statement data for one company from FMP.

    The function sends a GET request to the official endpoint, checks that
    the response format is valid, converts the payload into a DataFrame,
    parses date columns when available, and sorts the result by date.
    """
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
    """
    Download historical balance sheet data for one company from FMP.

    This is used together with the income statement endpoint so the final
    output contains both income statement variables and balance sheet variables.
    """
    return fetch_statement_data(
        endpoint_url=BALANCE_SHEET_URL,
        symbol=symbol,
        api_key=api_key,
        period=period,
        limit=limit,
        session=session,
    )


def fetch_statement_data(
    endpoint_url: str,
    symbol: str,
    api_key: str,
    period: str,
    limit: int,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """
    Download one financial statement dataset from a specific FMP endpoint.

    The same helper is reused for both income statement and balance sheet
    requests because the response structure is handled in the same way.
    """
    # Query parameters sent to FMP.
    params = {
        "symbol": symbol,
        "period": period,
        "limit": limit,
        "apikey": api_key,
    }

    # Reuse an existing session when available to reduce connection overhead.
    http = session or requests.Session()
    response = http.get(endpoint_url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, list):
        raise ValueError(f"Unexpected FMP response for {symbol}: {payload}")

    if not payload:
        return pd.DataFrame()

    # Create a tabular structure and keep track of the ticker used in the request.
    statement_df = pd.DataFrame(payload)
    statement_df.insert(0, "requested_symbol", symbol)

    # Convert date-like columns to pandas datetime when present.
    for date_col in ("date", "fillingDate", "acceptedDate"):
        if date_col in statement_df.columns:
            statement_df[date_col] = pd.to_datetime(statement_df[date_col], errors="coerce")

    # Sort from most recent to oldest statement.
    if "date" in statement_df.columns:
        statement_df = statement_df.sort_values("date", ascending=False).reset_index(drop=True)

    return statement_df


def select_requested_fields(
    statement_df: pd.DataFrame,
    field_map: dict[str, list[str]],
    base_columns: list[str],
) -> pd.DataFrame:
    """
    Keep only the requested variables and preserve FMP-style output names.

    Each output field can have multiple candidate source columns, because FMP
    may expose equivalent values with slightly different names across datasets.
    """
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
    """
    Remove duplicate rows for the same reporting period before merging.

    When duplicates exist, the function keeps the latest available row based on
    acceptedDate, filingDate, and date, in that order of preference.
    """
    if statement_df.empty or not key_columns:
        return statement_df

    sort_columns = [
        column
        for column in ("acceptedDate", "filingDate", "date")
        if column in statement_df.columns
    ]
    if sort_columns:
        deduplicated_df = statement_df.sort_values(sort_columns, ascending=False)
    else:
        deduplicated_df = statement_df.copy()

    return deduplicated_df.drop_duplicates(subset=key_columns, keep="first").reset_index(drop=True)


def merge_financial_statements(
    income_df: pd.DataFrame,
    balance_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge income statement data and balance sheet data for the same reporting periods.

    The final DataFrame contains only the requested FMP variables plus
    a compact set of metadata columns needed to identify each record.
    """
    merge_keys = [
        column
        for column in MERGE_KEY_CANDIDATES
        if column in income_df.columns and column in balance_df.columns
    ]
    if not merge_keys:
        raise ValueError("No common keys were found to merge income and balance data.")

    # Deduplicate both inputs first to avoid duplicated rows after the merge.
    income_df = deduplicate_statement_rows(income_df, merge_keys)
    balance_df = deduplicate_statement_rows(balance_df, merge_keys)

    # Keep richer metadata from the income statement side, then merge only the
    # requested balance sheet variables on a smaller and more stable key set.
    income_base_columns = [
        column for column in OUTPUT_METADATA_COLUMNS if column in income_df.columns
    ]
    income_selected = select_requested_fields(income_df, INCOME_FIELD_MAP, income_base_columns)
    balance_selected = select_requested_fields(balance_df, BALANCE_FIELD_MAP, merge_keys)

    # Use an inner merge so the output only contains periods where both
    # statements are available, avoiding partial rows with unnecessary NaNs.
    merged_df = income_selected.merge(balance_selected, on=merge_keys, how="inner")
    metadata_columns = [column for column in OUTPUT_METADATA_COLUMNS if column in merged_df.columns]
    ordered_columns = metadata_columns + list(INCOME_FIELD_MAP.keys()) + list(BALANCE_FIELD_MAP.keys())
    return merged_df[ordered_columns]


def download_income_statements(
    companies: pd.DataFrame,
    api_key: str,
    period: str = SELECTED_PERIOD,
    limit: int = STATEMENT_LIMIT,
    output_dir: Path = OUTPUT_DIR,
    pause_seconds: float = REQUEST_PAUSE_SECONDS,
) -> pd.DataFrame:
    """
    Download financial statement data for multiple companies and save the results.

    Output generated by this function:
    - one aggregated CSV file with all downloaded rows
    """
    # Save the aggregated dataset directly inside data/FMP.
    output_dir.mkdir(parents=True, exist_ok=True)

    combined_frames: list[pd.DataFrame] = []
    company_names = {}

    # Map ticker -> company name when the source DataFrame contains that column.
    if "Company_name" in companies.columns:
        company_names = companies.set_index("Ticker")["Company_name"].to_dict()

    # Use unique tickers only to avoid duplicate downloads.
    tickers = companies["Ticker"].dropna().astype(str).str.strip().unique()

    with requests.Session() as session:
        for ticker in tickers:
            # Download data company by company so errors on one ticker do not stop the whole process.
            print(f"Downloading financial statements for {ticker}...")
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

            # Add the company name when it is available in the input list.
            company_name = company_names.get(ticker)
            if company_name:
                company_df.insert(1, "company_name", company_name)

            # Keep the company data in memory and save only the final aggregated file.
            combined_frames.append(company_df)

            # Short pause between requests to be gentler with the API.
            time.sleep(pause_seconds)

    if not combined_frames:
        return pd.DataFrame()

    # Merge all company DataFrames into a single dataset for downstream analysis.
    combined_df = pd.concat(combined_frames, ignore_index=True)
    combined_file = output_dir / f"income_statements_{period}_all_companies.csv"
    combined_df.to_csv(combined_file, index=False)
    print(f"Saved aggregated file: {combined_file}")

    return combined_df



def main() -> None:
    """
    Main execution flow of the script.

    Steps:
    1. Check that the API key and configuration are valid
    2. Decide whether to use manual tickers or the CSV file
    3. Download the statements
    4. Print a final summary
    """
    # Ensure the API key has been explicitly set in the source code.
    if not FMP_API_KEY or FMP_API_KEY == "INSERISCI_LA_TUA_API_KEY":
        raise EnvironmentError(
            "Set your FMP_API_KEY directly in this file before running the script."
        )

    # Validate settings before any network request is sent.
    validate_settings()

    # Use manual tickers when provided, otherwise read the first N companies from the CSV.
    if SELECTED_TICKERS:
        companies = build_companies_from_tickers(SELECTED_TICKERS)
    else:
        companies = load_companies(limit=SELECTED_COMPANY_LIMIT)

    # Download and save the requested income statement data.
    combined_df = download_income_statements(
        companies=companies,
        api_key=FMP_API_KEY,
        period=SELECTED_PERIOD,
        limit=STATEMENT_LIMIT,
        output_dir=OUTPUT_DIR,
    )

    if combined_df.empty:
        print("Download completed, but no records were saved.")
        return

    # Print a compact summary of the completed download.
    print(
        "Download completed: "
        f"{combined_df['requested_symbol'].nunique()} ticker, "
        f"{len(combined_df)} total rows."
    )


# Run the script only when this file is executed directly.
if __name__ == "__main__":
    main()


# %%
