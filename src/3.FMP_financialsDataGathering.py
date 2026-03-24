#%%
import time

import pandas as pd
import requests
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
OUTPUT_FILE = cfg.FMP_RAW_FINANCIALS


# ---------------------------------------------------------------------------
# API Settings
# ---------------------------------------------------------------------------

FMP_API_KEY = "af6MfImMPNcg8od1SarpRna0ZY61vZT7"
# Leave this empty to load the first ENTERPRISE_ROW_LIMIT companies from enterprises.csv.
SELECTED_TICKERS: list[str] = []
ENTERPRISE_ROW_LIMIT = 110

SELECTED_PERIOD = "quarter"
# Pull a longer quarterly history so trailing-twelve-month factors are already
# available when the weekly sample starts in early 2021.
STATEMENT_LIMIT = 60
REQUEST_TIMEOUT = 30
REQUEST_PAUSE_SECONDS = 0.25


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


# ---------------------------------------------------------------------------
# Requested Financial Fields
# ---------------------------------------------------------------------------

INCOME_FIELD_MAP = {
    "revenue": ["revenue"],
    "grossProfit": ["grossProfit"],
    "operatingIncome": ["operatingIncome"],
    "netIncome": ["netIncome", "bottomLineNetIncome"],
    "interestExpense": ["interestExpense"],
    # Keep both share-count fields so the processing step can choose the
    # version that best approximates market cap.
    "weightedAverageShsOut": ["weightedAverageShsOut"],
    "weightedAverageShsOutDil": ["weightedAverageShsOutDil"],
}

BALANCE_FIELD_MAP = {
    "totalAssets": ["totalAssets"],
    "totalStockholdersEquity": ["totalStockholdersEquity", "totalEquity"],
    "totalCurrentAssets": ["totalCurrentAssets", "currentAssets"],
    "totalCurrentLiabilities": ["totalCurrentLiabilities", "currentLiabilities"],
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
                # Download Financial Statements
                # -----------------------------------------------------------

                statement_frames = {}

                for statement_name, endpoint_url in {
                    "income": INCOME_STATEMENT_URL,
                    "balance": BALANCE_SHEET_URL,
                    "cash_flow": CASH_FLOW_STATEMENT_URL,
                    "enterprise_values": ENTERPRISE_VALUES_URL,
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

                    if isinstance(payload, list):
                        records = payload
                    elif isinstance(payload, dict) and isinstance(
                        payload.get("historical"),
                        list,
                    ):
                        records = payload["historical"]
                        if payload.get("symbol"):
                            records = [
                                {**record, "symbol": payload["symbol"]}
                                for record in records
                            ]
                    else:
                        raise ValueError(f"Unexpected FMP response for {ticker}: {payload}")

                    if not records:
                        statement_frames[statement_name] = pd.DataFrame()
                        continue

                    statement_df = pd.DataFrame(records)
                    statement_df.insert(0, "requested_symbol", ticker)

                    if "symbol" not in statement_df.columns:
                        statement_df.insert(1, "symbol", ticker)

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
            cash_flow_df = statement_frames["cash_flow"]
            enterprise_values_df = statement_frames["enterprise_values"]

            if income_df.empty:
                print(f"No income statement data available for {ticker}")
                continue

            if balance_df.empty:
                print(f"No balance sheet data available for {ticker}")
                continue

            if cash_flow_df.empty:
                print(f"No cash flow statement data available for {ticker}")
                continue
            if enterprise_values_df.empty:
                print(f"No enterprise values data available for {ticker}")
                continue

            # ---------------------------------------------------------------
            # Build Merge Keys
            # ---------------------------------------------------------------

            income_balance_merge_keys = [
                column
                for column in MERGE_KEY_CANDIDATES
                if column in income_df.columns and column in balance_df.columns
            ]
            if not income_balance_merge_keys:
                print(
                    f"Data error for {ticker}: no common keys were found to merge income and balance data."
                )
                continue

            income_cash_flow_merge_keys = [
                column
                for column in MERGE_KEY_CANDIDATES
                if column in income_df.columns and column in cash_flow_df.columns
            ]
            if not income_cash_flow_merge_keys:
                print(
                    f"Data error for {ticker}: no common keys were found to merge income and cash flow data."
                )
                continue

            income_enterprise_merge_keys = [
                column
                for column in MERGE_KEY_CANDIDATES
                if column in income_df.columns and column in enterprise_values_df.columns
            ]
            if not income_enterprise_merge_keys:
                print(
                    f"Data error for {ticker}: no common keys were found to merge income and enterprise values data."
                )
                continue

            # ---------------------------------------------------------------
            # Deduplicate Statements
            # ---------------------------------------------------------------

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

            balance_selected = balance_df[income_balance_merge_keys].copy()
            for output_name, candidate_columns in BALANCE_FIELD_MAP.items():
                source_column = next(
                    (column for column in candidate_columns if column in balance_df.columns),
                    None,
                )
                balance_selected[output_name] = (
                    balance_df[source_column] if source_column is not None else pd.NA
                )

            cash_flow_selected = cash_flow_df[income_cash_flow_merge_keys].copy()
            for output_name, candidate_columns in CASH_FLOW_FIELD_MAP.items():
                source_column = next(
                    (column for column in candidate_columns if column in cash_flow_df.columns),
                    None,
                )
                cash_flow_selected[output_name] = (
                    cash_flow_df[source_column] if source_column is not None else pd.NA
                )

            enterprise_values_selected = enterprise_values_df[income_enterprise_merge_keys].copy()
            for output_name, candidate_columns in ENTERPRISE_VALUE_FIELD_MAP.items():
                source_column = next(
                    (
                        column
                        for column in candidate_columns
                        if column in enterprise_values_df.columns
                    ),
                    None,
                )
                enterprise_values_selected[output_name] = (
                    enterprise_values_df[source_column] if source_column is not None else pd.NA
                )

            # ---------------------------------------------------------------
            # Merge And Save In Memory
            # ---------------------------------------------------------------

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
