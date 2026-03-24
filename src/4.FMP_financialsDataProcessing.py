#%%
import subprocess
import sys

import pandas as pd
import config as cfg


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Set this to True when you want the script to re-download the raw FMP data first.
REDOWNLOAD_RAW_FMP_DATA = False

# Input/output paths used by the processing step.
RAW_INPUT_FILE = cfg.FMP_RAW_FINANCIALS
OUTPUT_FILE = cfg.FMP_FINANCIALS
SINGLE_COMPANY_OUTPUT_DIR = cfg.SINGLE_COMPANY_FINANCIALS

# Raw columns that must be parsed as dates before alignment.
DATE_COLUMNS = ["date", "filingDate", "acceptedDate"]
# Metadata columns that should not appear in the final processed datasets.
COLUMNS_TO_DROP = [
    "requested_symbol",
    "date",
    "fiscalYear",
    "period",
    "filingDate",
    "acceptedDate",
    "reportedCurrency",
    "cik",
]


# ---------------------------------------------------------------------------
# Optional Raw FMP Download
# ---------------------------------------------------------------------------

# Re-run the raw download script before processing when explicitly requested.
if REDOWNLOAD_RAW_FMP_DATA:
    subprocess.run(
        [sys.executable, str(cfg.SRC / "FMP_financialsDataGathering.py")],
        check=True,
    )


# ---------------------------------------------------------------------------
# Load Raw FMP Data
# ---------------------------------------------------------------------------

# Stop immediately if the raw FMP dataset has not been created yet.
if not RAW_INPUT_FILE.exists():
    raise FileNotFoundError(f"Raw financials file not found: {RAW_INPUT_FILE}")

# Load the raw quarterly financial statements downloaded from FMP.
raw_df = pd.read_csv(RAW_INPUT_FILE)
# The raw file must contain the ticker used for grouping company by company.
if "requested_symbol" not in raw_df.columns:
    raise KeyError("The raw financials file must contain a 'requested_symbol' column.")

# Convert available date columns to pandas datetime.
for column in DATE_COLUMNS:
    if column in raw_df.columns:
        raw_df[column] = pd.to_datetime(raw_df[column], errors="coerce")


# ---------------------------------------------------------------------------
# Prepare Output Folders
# ---------------------------------------------------------------------------

# Ensure the output folders exist before exporting any processed files.
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
SINGLE_COMPANY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Collect all aligned company DataFrames to build the final aggregated dataset.
combined_frames: list[pd.DataFrame] = []


# ---------------------------------------------------------------------------
# Process Each Company
# ---------------------------------------------------------------------------

# Process one ticker at a time.
for ticker, company_df in raw_df.groupby("requested_symbol", sort=True):
    print(f"Aligning financial statements for {ticker}...")

    # -----------------------------------------------------------------------
    # Load Price Calendar
    # -----------------------------------------------------------------------

    # Load the weekly price calendar already generated for the same ticker.
    price_file = cfg.SINGLE_COMPANY_PRICES / f"{ticker}Prices.csv"
    if not price_file.exists():
        print(f"Price calendar error for {ticker}: file not found: {price_file}")
        continue

    # The price file provides the canonical WeekEndingFriday index.
    price_df = pd.read_csv(price_file, parse_dates=["WeekEndingFriday"])
    if "WeekEndingFriday" not in price_df.columns:
        print(
            f"Price calendar error for {ticker}: "
            f"'WeekEndingFriday' column not found in {price_file}"
        )
        continue

    # Build the weekly index shared with the price dataset.
    price_index = pd.DatetimeIndex(price_df["WeekEndingFriday"]).sort_values().unique()
    price_index.name = "WeekEndingFriday"

    # -----------------------------------------------------------------------
    # Align FMP Dates To WeekEndingFriday
    # -----------------------------------------------------------------------

    # Start from an empty weekly grid and fill only the rows matching financial dates.
    aligned_df = pd.DataFrame(index=price_index)
    aligned_df.index.name = "WeekEndingFriday"

    # Work on a copy so the original grouped slice is not modified in place.
    working_df = company_df.copy()
    # Use the FMP statement date as the reference date for alignment.
    parsed_dates = pd.to_datetime(working_df["date"], errors="coerce")
    valid_dates = parsed_dates.notna()

    # Initialize the aligned weekly date column.
    working_df["WeekEndingFriday"] = pd.NaT
    if valid_dates.any():
        # Compute the previous and next Friday for every valid statement date.
        valid_parsed_dates = parsed_dates.loc[valid_dates]
        weekday = valid_parsed_dates.dt.weekday
        days_since_prev_friday = (weekday - 4) % 7
        days_until_next_friday = (4 - weekday) % 7

        previous_friday = valid_parsed_dates - pd.to_timedelta(
            days_since_prev_friday,
            unit="D",
        )
        next_friday = valid_parsed_dates + pd.to_timedelta(
            days_until_next_friday,
            unit="D",
        )
        # Choose the closest Friday and normalize the timestamp to midnight.
        use_previous_friday = days_since_prev_friday <= days_until_next_friday
        nearest_friday = previous_friday.where(use_previous_friday, next_friday).dt.normalize()
        working_df.loc[valid_dates, "WeekEndingFriday"] = nearest_friday

    # Remove rows where the source date could not be aligned.
    working_df = working_df.dropna(subset=["WeekEndingFriday"])

    # -----------------------------------------------------------------------
    # Deduplicate And Join
    # -----------------------------------------------------------------------

    # When multiple rows map to the same Friday, keep the most recent filing metadata.
    sort_columns = [
        column
        for column in ("acceptedDate", "filingDate", "date")
        if column in working_df.columns
    ]
    if sort_columns:
        working_df = working_df.sort_values(sort_columns, ascending=False)

    # Keep only one financial row per WeekEndingFriday and use that date as index.
    working_df = (
        working_df.drop_duplicates(subset=["WeekEndingFriday"], keep="first")
        .set_index("WeekEndingFriday")
        .sort_index()
    )

    # Join quarterly financial data onto the weekly price calendar.
    aligned_df = aligned_df.join(working_df, how="left")

    # If the first weekly row is still empty, fill it with the latest raw
    # financial observation available before the first calendar date.
    first_week = aligned_df.index.min()
    previous_rows = working_df[working_df.index < first_week]
    if not previous_rows.empty:
        previous_row = previous_rows.iloc[-1]
        first_row_is_empty = aligned_df.loc[first_week].isna().all()
        if first_row_is_empty:
            aligned_df.loc[first_week, previous_row.index] = previous_row.values

    # -----------------------------------------------------------------------
    # Clean And Reorder Columns
    # -----------------------------------------------------------------------

    # Ensure the company ticker is available on every row of the aligned dataset.
    if "symbol" in aligned_df.columns:
        aligned_df["symbol"] = aligned_df["symbol"].fillna(ticker)
    else:
        aligned_df["symbol"] = ticker

    # Propagate the company name across all weekly rows when available.
    if "company_name" in aligned_df.columns:
        company_name = company_df["company_name"].dropna()
        if not company_name.empty:
            aligned_df["company_name"] = aligned_df["company_name"].fillna(
                company_name.iloc[0]
            )

    # -----------------------------------------------------------------------
    # Build Filled Financial Columns
    # -----------------------------------------------------------------------

    # Keep only the identifier columns outside the financial transformation.
    identifier_columns = [
        column for column in ("company_name", "symbol") if column in aligned_df.columns
    ]

    # Remove technical and metadata columns not needed in the final outputs.
    aligned_df = aligned_df.drop(
        columns=[column for column in COLUMNS_TO_DROP if column in aligned_df.columns]
    )

    # Identify the financial columns that must be forward-filled.
    financial_columns = [
        column for column in aligned_df.columns if column not in identifier_columns
    ]

    # Fill the base financial columns with the latest available real observation.
    real_financial_df = aligned_df[financial_columns].copy()
    aligned_df[financial_columns] = real_financial_df.ffill()

    # Keep company identifiers first, then all financial variables.
    ordered_columns = identifier_columns
    remaining_columns = [
        column for column in aligned_df.columns if column not in ordered_columns
    ]
    aligned_df = aligned_df[ordered_columns + remaining_columns]

    # -----------------------------------------------------------------------
    # Save Company Output
    # -----------------------------------------------------------------------

    # Save the processed company-level file.
    company_output_file = SINGLE_COMPANY_OUTPUT_DIR / f"{ticker}Financials.csv"
    aligned_df.to_csv(company_output_file, index=True)
    print(f"Saved company file: {company_output_file}")
    combined_frames.append(aligned_df)


# ---------------------------------------------------------------------------
# Save Final Aggregated Output
# ---------------------------------------------------------------------------

# If nothing was processed successfully, report it and stop.
if not combined_frames:
    print("Processing completed, but no aligned records were saved.")
else:
    # Merge all processed company files into one final dataset.
    combined_df = pd.concat(combined_frames).sort_index()
    combined_df.to_csv(OUTPUT_FILE, index=True)
    print(f"Saved aligned file: {OUTPUT_FILE}")
    # Print a compact summary of the processing step.
    print(
        "Processing completed: "
        f"{raw_df['requested_symbol'].nunique()} ticker, "
        f"{len(combined_df)} total rows."
    )
#%%

import matplotlib.pyplot as plt

enterprises_df = pd.read_csv(cfg.ENT)

for ticker in enterprises_df["Ticker"]:

    # Select the company to visualize.
    PLOT_TICKER = ticker 
    # Select the financial feature to visualize.
    PLOT_FEATURE = "revenue"

    # Load the processed company financial dataset for a quick visual check.
    company_plot_df = pd.read_csv(
        cfg.SINGLE_COMPANY_FINANCIALS / f"{PLOT_TICKER}Financials.csv",
        parse_dates=["WeekEndingFriday"],
    )

    # Plot the forward-filled feature on the weekly aligned calendar.
    plt.figure(figsize=(12, 6))
    plt.plot(
        company_plot_df["WeekEndingFriday"],
        company_plot_df[PLOT_FEATURE],
        label=PLOT_FEATURE,
        linewidth=2,
    )
    plt.title(
        f"{enterprises_df[enterprises_df['Ticker'] == PLOT_TICKER]['companyName'].iloc[0]} {PLOT_FEATURE}"
    )
    plt.xlabel("WeekEndingFriday")
    plt.ylabel(PLOT_FEATURE)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

# %%
