#%%
import time

import pandas as pd
import requests
import config as cfg


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INCOME_STATEMENT_URL = "https://financialmodelingprep.com/stable/income-statement"
BALANCE_SHEET_URL = "https://financialmodelingprep.com/stable/balance-sheet-statement"

ENTERPRISES_PATH = cfg.ENT
OUTPUT_FILE = cfg.FMP_RAW_FINANCIALS

FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
# Leave this empty to load the first ENTERPRISE_ROW_LIMIT companies from enterprises.csv.
SELECTED_TICKERS: list[str] = []
ENTERPRISE_ROW_LIMIT = 110

SELECTED_PERIOD = "quarter"
STATEMENT_LIMIT = 25
REQUEST_TIMEOUT = 30
REQUEST_PAUSE_SECONDS = 0.25

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


def main() -> None:
    # -----------------------------------------------------------------------
    # Validate Settings
    # -----------------------------------------------------------------------

    # Stop immediately if the API key has not been configured.
    if not FMP_API_KEY or FMP_API_KEY == "INSERISCI_LA_TUA_API_KEY":
        raise EnvironmentError(
            "Set your FMP_API_KEY directly in this file before running the script."
        )

    # Validate the selected period and row limit.
    if SELECTED_PERIOD not in {"annual", "quarter"}:
        raise ValueError("SELECTED_PERIOD must be either 'annual' or 'quarter'.")

    if ENTERPRISE_ROW_LIMIT is not None and ENTERPRISE_ROW_LIMIT < 0:
        raise ValueError("ENTERPRISE_ROW_LIMIT must be greater than or equal to 0.")

    # -----------------------------------------------------------------------
    # Load Company List
    # -----------------------------------------------------------------------

    # Use manually selected tickers when provided.
    if SELECTED_TICKERS:
        cleaned_tickers = [
            ticker.strip().upper()
            for ticker in SELECTED_TICKERS
            if ticker and ticker.strip()
        ]
        if not cleaned_tickers:
            raise ValueError("The ticker list is empty.")

        companies = pd.DataFrame({"Ticker": cleaned_tickers})

    # Otherwise load the first N valid rows from enterprises.csv.
    else:
        if not ENTERPRISES_PATH.exists():
            raise FileNotFoundError(f"Company file not found: {ENTERPRISES_PATH}")

        companies = pd.read_csv(ENTERPRISES_PATH)
        if "Ticker" not in companies.columns:
            raise KeyError("The 'Ticker' column does not exist in enterprises.csv")

        companies = companies.dropna(subset=["Ticker"]).copy()
        companies["Ticker"] = companies["Ticker"].astype(str).str.strip()
        companies = companies[companies["Ticker"] != ""]

        if ENTERPRISE_ROW_LIMIT is not None:
            companies = companies.head(ENTERPRISE_ROW_LIMIT).copy()

    # -----------------------------------------------------------------------
    # Prepare Output
    # -----------------------------------------------------------------------

    # Ensure the output folder exists before writing the raw CSV.
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Keep all downloaded company DataFrames to build the final raw dataset.
    combined_frames: list[pd.DataFrame] = []

    # Recover company names when available in the enterprises dataset.
    company_names = {}
    if "Company_name" in companies.columns:
        company_names = companies.set_index("Ticker")["Company_name"].to_dict()

    # Use unique tickers only to avoid duplicate downloads.
    tickers = companies["Ticker"].dropna().astype(str).str.strip().unique()

    # -----------------------------------------------------------------------
    # Download And Merge FMP Data
    # -----------------------------------------------------------------------

    with requests.Session() as session:
        for ticker in tickers:
            print(f"Downloading raw financial statements for {ticker}...")

            try:
                # -----------------------------------------------------------
                # Download Income Statement And Balance Sheet
                # -----------------------------------------------------------

                statement_frames = {}

                for statement_name, endpoint_url in {
                    "income": INCOME_STATEMENT_URL,
                    "balance": BALANCE_SHEET_URL,
                }.items():
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
                    response.raise_for_status()
                    payload = response.json()

                    if not isinstance(payload, list):
                        raise ValueError(f"Unexpected FMP response for {ticker}: {payload}")

                    if not payload:
                        statement_frames[statement_name] = pd.DataFrame()
                        continue

                    statement_df = pd.DataFrame(payload)
                    statement_df.insert(0, "requested_symbol", ticker)

                    for date_col in ("date", "filingDate", "acceptedDate", "fillingDate"):
                        if date_col in statement_df.columns:
                            statement_df[date_col] = pd.to_datetime(
                                statement_df[date_col],
                                errors="coerce",
                            )

                    if "date" in statement_df.columns:
                        statement_df = statement_df.sort_values(
                            "date",
                            ascending=False,
                        ).reset_index(drop=True)

                    statement_frames[statement_name] = statement_df

            except requests.HTTPError as exc:
                print(f"HTTP error for {ticker}: {exc}")
                continue
            except requests.RequestException as exc:
                print(f"Network error for {ticker}: {exc}")
                continue
            except ValueError as exc:
                print(f"Data error for {ticker}: {exc}")
                continue

            income_df = statement_frames["income"]
            balance_df = statement_frames["balance"]

            if income_df.empty:
                print(f"No income statement data available for {ticker}")
                continue

            if balance_df.empty:
                print(f"No balance sheet data available for {ticker}")
                continue

            # ---------------------------------------------------------------
            # Build Merge Keys
            # ---------------------------------------------------------------

            merge_keys = [
                column
                for column in MERGE_KEY_CANDIDATES
                if column in income_df.columns and column in balance_df.columns
            ]
            if not merge_keys:
                print(f"Data error for {ticker}: no common keys were found to merge data.")
                continue

            # ---------------------------------------------------------------
            # Deduplicate Both Statements
            # ---------------------------------------------------------------

            for statement_df in (income_df, balance_df):
                sort_columns = [
                    column
                    for column in ("acceptedDate", "filingDate", "fillingDate", "date")
                    if column in statement_df.columns
                ]
                if sort_columns:
                    statement_df.sort_values(sort_columns, ascending=False, inplace=True)

            income_df = income_df.drop_duplicates(subset=merge_keys, keep="first").reset_index(drop=True)
            balance_df = balance_df.drop_duplicates(subset=merge_keys, keep="first").reset_index(drop=True)

            # ---------------------------------------------------------------
            # Select Requested Fields
            # ---------------------------------------------------------------

            income_base_columns = [
                column for column in OUTPUT_METADATA_COLUMNS if column in income_df.columns
            ]
            income_selected = income_df[income_base_columns].copy()
            for output_name, candidate_columns in INCOME_FIELD_MAP.items():
                source_column = next(
                    (column for column in candidate_columns if column in income_df.columns),
                    None,
                )
                income_selected[output_name] = (
                    income_df[source_column] if source_column is not None else pd.NA
                )

            balance_selected = balance_df[merge_keys].copy()
            for output_name, candidate_columns in BALANCE_FIELD_MAP.items():
                source_column = next(
                    (column for column in candidate_columns if column in balance_df.columns),
                    None,
                )
                balance_selected[output_name] = (
                    balance_df[source_column] if source_column is not None else pd.NA
                )

            # ---------------------------------------------------------------
            # Merge And Save In Memory
            # ---------------------------------------------------------------

            company_df = income_selected.merge(balance_selected, on=merge_keys, how="inner")

            ordered_columns = (
                [column for column in OUTPUT_METADATA_COLUMNS if column in company_df.columns]
                + list(INCOME_FIELD_MAP.keys())
                + list(BALANCE_FIELD_MAP.keys())
            )
            company_df = company_df[ordered_columns]

            company_name = company_names.get(ticker)
            if company_name:
                company_df.insert(1, "company_name", company_name)

            combined_frames.append(company_df)
            time.sleep(REQUEST_PAUSE_SECONDS)

    # -----------------------------------------------------------------------
    # Save Final Raw Output
    # -----------------------------------------------------------------------

    if not combined_frames:
        print("Download completed, but no raw records were saved.")
        return

    combined_df = pd.concat(combined_frames, ignore_index=True)

    if {"requested_symbol", "date"}.issubset(combined_df.columns):
        combined_df = combined_df.sort_values(
            ["requested_symbol", "date"],
            ascending=[True, False],
        ).reset_index(drop=True)
    elif "requested_symbol" in combined_df.columns:
        combined_df = combined_df.sort_values("requested_symbol").reset_index(drop=True)

    combined_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved raw file: {OUTPUT_FILE}")
    print(
        "Raw download completed: "
        f"{combined_df['requested_symbol'].nunique()} ticker, "
        f"{len(combined_df)} total rows."
    )


if __name__ == "__main__":
    main()


# %%
