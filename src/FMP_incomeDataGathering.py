#%%
import time
from pathlib import Path

import pandas as pd
import requests
import config as cfg


# Base endpoint for the Financial Modeling Prep income statement API.
BASE_URL = "https://financialmodelingprep.com/stable/income-statement"

# Project paths are centralized in config.py to avoid duplicating path logic here.
ENTERPRISES_PATH = cfg.ENT
OUTPUT_DIR = cfg.FMP_INCOME

# Main script settings.
# Fill in your FMP API key here before running the script.
FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"

# If this list is not empty, the script downloads data only for these tickers.
# If it is empty, the script loads companies from the enterprises CSV file.
SELECTED_TICKERS: list[str] = ["GS"]

# Financial statement frequency requested from FMP.
SELECTED_PERIOD = "quarter"

# Maximum number of statements requested per company.
STATEMENT_LIMIT = 5

# Number of companies to load from the project CSV when SELECTED_TICKERS is empty.
SELECTED_COMPANY_LIMIT = 10

# HTTP timeout for each API request.
REQUEST_TIMEOUT = 30

# Current subscription constraint for the "limit" parameter.
MAX_SUBSCRIPTION_LIMIT = 5


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
    if STATEMENT_LIMIT < 0 or STATEMENT_LIMIT > MAX_SUBSCRIPTION_LIMIT:
        raise ValueError(
            f"STATEMENT_LIMIT must be between 0 and {MAX_SUBSCRIPTION_LIMIT} "
            "for the current FMP subscription."
        )


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
    # Query parameters sent to FMP.
    params = {
        "symbol": symbol,
        "period": period,
        "limit": limit,
        "apikey": api_key,
    }

    # Reuse an existing session when available to reduce connection overhead.
    http = session or requests.Session()
    response = http.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, list):
        raise ValueError(f"Unexpected FMP response for {symbol}: {payload}")

    if not payload:
        return pd.DataFrame()

    # Create a tabular structure and keep track of the ticker used in the request.
    income_df = pd.DataFrame(payload)
    income_df.insert(0, "requested_symbol", symbol)

    # Convert date-like columns to pandas datetime when present.
    for date_col in ("date", "fillingDate", "acceptedDate"):
        if date_col in income_df.columns:
            income_df[date_col] = pd.to_datetime(income_df[date_col], errors="coerce")

    # Sort from most recent to oldest statement.
    if "date" in income_df.columns:
        income_df = income_df.sort_values("date", ascending=False).reset_index(drop=True)

    return income_df


def download_income_statements(
    companies: pd.DataFrame,
    api_key: str,
    period: str = SELECTED_PERIOD,
    limit: int = STATEMENT_LIMIT,
    output_dir: Path = OUTPUT_DIR,
    pause_seconds: float = 0.25,
) -> pd.DataFrame:
    """
    Download income statements for multiple companies and save the results.

    Output generated by this function:
    - one CSV file for each ticker
    - one aggregated CSV file with all downloaded rows
    """
    # Organize files by frequency, for example: .../income_statements/quarter/
    period_dir = output_dir / period
    period_dir.mkdir(parents=True, exist_ok=True)

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
            print(f"Downloading income statement for {ticker}...")
            try:
                income_df = fetch_income_statement(
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
                print(f"No data available for {ticker}")
                continue

            # Add the company name when it is available in the input list.
            company_name = company_names.get(ticker)
            if company_name:
                income_df.insert(1, "company_name", company_name)

            # Save one CSV per company for easier inspection and reuse.
            company_file = period_dir / f"{ticker}_income_statement.csv"
            income_df.to_csv(company_file, index=False)
            combined_frames.append(income_df)

            print(f"Saved: {company_file}")

            # Short pause between requests to be gentler with the API.
            time.sleep(pause_seconds)

    if not combined_frames:
        return pd.DataFrame()

    # Merge all company DataFrames into a single dataset for downstream analysis.
    combined_df = pd.concat(combined_frames, ignore_index=True)
    combined_file = period_dir / f"income_statements_{period}_all_companies.csv"
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
