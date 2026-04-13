#%%
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config as cfg


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATE_COLUMN = "WeekEndingFriday"
TICKER_COLUMN = "Ticker"
ENTERPRISE_METADATA_COLUMNS = ["SectorCode"]

ENTERPRISES_FILE = cfg.ENT
PRICE_INPUT_DIR = cfg.SINGLE_COMPANY_PRICES
FINANCIAL_INPUT_DIR = cfg.SINGLE_COMPANY_FINANCIALS
SINGLE_COMPANY_OUTPUT_DIR = cfg.SINGLE_COMPANY_FULL_DATA
OUTPUT_FILE = cfg.FULL_DATA
ML_OUTPUT_FILE = cfg.FULL_DATA_ML


# ---------------------------------------------------------------------------
# Load Company List
# ---------------------------------------------------------------------------

if not ENTERPRISES_FILE.exists():
    raise FileNotFoundError(f"Company file not found: {ENTERPRISES_FILE}")

companies = pd.read_csv(ENTERPRISES_FILE)
if TICKER_COLUMN not in companies.columns:
    raise KeyError("The 'Ticker' column does not exist in enterprises.csv")

missing_enterprise_metadata_columns = [
    column for column in ENTERPRISE_METADATA_COLUMNS if column not in companies.columns
]
if missing_enterprise_metadata_columns:
    raise KeyError(
        "The following enterprise metadata columns are missing from enterprises.csv: "
        f"{missing_enterprise_metadata_columns}"
    )

tickers = (
    companies[TICKER_COLUMN]
    .dropna()
    .astype(str)
    .str.strip()
)
tickers = tickers[tickers != ""].unique()
enterprise_metadata_df = (
    companies[[TICKER_COLUMN] + ENTERPRISE_METADATA_COLUMNS]
    .drop_duplicates(subset=[TICKER_COLUMN], keep="first")
    .copy()
)


# ---------------------------------------------------------------------------
# Prepare Output Folders
# ---------------------------------------------------------------------------

SINGLE_COMPANY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
ML_OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

combined_frames: list[pd.DataFrame] = []


# ---------------------------------------------------------------------------
# Merge Price And Financial Files
# ---------------------------------------------------------------------------

for ticker in tickers:
    print(f"Merging price and financial data for {ticker}...")

    price_file = PRICE_INPUT_DIR / f"{ticker}Prices.csv"
    financial_file = FINANCIAL_INPUT_DIR / f"{ticker}Financials.csv"

    if not price_file.exists():
        print(f"Price file not found for {ticker}: {price_file}")
        continue

    if not financial_file.exists():
        print(f"Financial file not found for {ticker}: {financial_file}")
        continue

    price_df = pd.read_csv(price_file, parse_dates=[DATE_COLUMN])
    financial_df = pd.read_csv(financial_file, parse_dates=[DATE_COLUMN])

    # Drop the unadjusted close series from the final merged dataset and keep
    # only the adjusted-price side for modeling.
    price_columns_to_drop = {
        "closeprice",
        "closeprice_t-1",
        "closeprice_t-2",
        "closeprice_t+1",
    }
    price_df = price_df.drop(
        columns=[
            column
            for column in price_df.columns
            if column.lower() in price_columns_to_drop
        ],
        errors="ignore",
    )

    if DATE_COLUMN not in price_df.columns:
        print(f"Date column not found in price file for {ticker}: {price_file}")
        continue

    if DATE_COLUMN not in financial_df.columns:
        print(f"Date column not found in financial file for {ticker}: {financial_file}")
        continue

    if TICKER_COLUMN not in price_df.columns:
        price_df[TICKER_COLUMN] = ticker
    else:
        price_df[TICKER_COLUMN] = price_df[TICKER_COLUMN].fillna(ticker)

    if "symbol" in financial_df.columns:
        financial_df = financial_df.rename(columns={"symbol": TICKER_COLUMN})
    elif TICKER_COLUMN not in financial_df.columns:
        financial_df[TICKER_COLUMN] = ticker

    financial_df[TICKER_COLUMN] = financial_df[TICKER_COLUMN].fillna(ticker)

    price_df = (
        price_df.drop_duplicates(subset=[DATE_COLUMN, TICKER_COLUMN], keep="first")
        .sort_values([TICKER_COLUMN, DATE_COLUMN])
        .reset_index(drop=True)
    )
    financial_df = (
        financial_df.drop_duplicates(subset=[DATE_COLUMN, TICKER_COLUMN], keep="first")
        .sort_values([TICKER_COLUMN, DATE_COLUMN])
        .reset_index(drop=True)
    )

    full_df = price_df.merge(
        financial_df,
        on=[DATE_COLUMN, TICKER_COLUMN],
        how="inner",
    )

    full_df = full_df.merge(
        enterprise_metadata_df,
        on=TICKER_COLUMN,
        how="left",
    )

    if full_df.empty:
        print(f"No overlapping weekly rows found for {ticker}")
        continue

    # Drop human-readable company-name columns from the final merged datasets.
    full_df = full_df.drop(columns=["company_name", "companyName"], errors="ignore")

    full_df = full_df.sort_values([TICKER_COLUMN, DATE_COLUMN]).reset_index(drop=True)
    leading_columns = [
        DATE_COLUMN,
        TICKER_COLUMN,
        "AdjClosePrice_t+1",
        "AdjClosePrice_t+1_Up",
    ]
    remaining_columns = [
        column for column in full_df.columns if column not in leading_columns
    ]
    full_df = full_df[leading_columns + remaining_columns]

    company_output_file = SINGLE_COMPANY_OUTPUT_DIR / f"{ticker}data.csv"
    full_df.to_csv(company_output_file, index=False)
    combined_frames.append(full_df)





# ---------------------------------------------------------------------------
# Save Final Aggregated Output
# ---------------------------------------------------------------------------

if not combined_frames:
    print("Merge completed, but no merged records were saved.")
else:
    combined_df = pd.concat(combined_frames, ignore_index=True)
    combined_df = combined_df.sort_values([TICKER_COLUMN, DATE_COLUMN]).reset_index(
        drop=True
    )
    combined_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved merged full dataset: {OUTPUT_FILE}")
    print(
        "Merge completed: "
        f"{combined_df[TICKER_COLUMN].nunique()} ticker, "
        f"{len(combined_df)} total rows."
    )



# %%
