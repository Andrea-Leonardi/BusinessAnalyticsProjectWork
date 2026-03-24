#%%
import subprocess
import sys

import pandas as pd
import config as cfg


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Set this to True when you want the script to re-download the raw FMP data first.
REDOWNLOAD_RAW_FMP_DATA = True

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

DERIVED_FEATURE_COLUMNS = [
    "BookToMarket",
    "MarketCap",
    "GrossProfitability",
    "OperatingMargin",
    "ROA",
    "ROE",
    "AssetGrowth",
    "InvestmentIntensity",
    "Accruals",
    "IncomeQuality",
    "DebtToAssets",
    "InterestCoverage",
    "CashRatio",
    "WorkingCapitalScaled",
    "FreeCashFlowYield",
    "EarningsYield",
]


# ---------------------------------------------------------------------------
# Utility Helpers
# ---------------------------------------------------------------------------

# Safely divide two series while turning zero denominators into missing values.
def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.mask(denominator == 0)
    result = numerator.divide(denominator)
    return result.where(pd.notna(denominator))


# Retrieve a numeric series from the aligned dataset and return an empty series
# if the source column is missing for a given company.
def get_numeric_series(dataframe: pd.DataFrame, column: str) -> pd.Series:
    if column not in dataframe.columns:
        return pd.Series(index=dataframe.index, dtype="float64")

    return pd.to_numeric(dataframe[column], errors="coerce")


# ---------------------------------------------------------------------------
# Optional Raw FMP Download
# ---------------------------------------------------------------------------

# Re-run the raw download script before processing when explicitly requested.
if REDOWNLOAD_RAW_FMP_DATA:
    subprocess.run(
        [sys.executable, str(cfg.SRC / "3.FMP_financialsDataGathering.py")],
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
    if "ClosePrice" not in price_df.columns:
        print(
            f"Price calendar error for {ticker}: "
            f"'ClosePrice' column not found in {price_file}"
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

    # Convert the raw financial fields to numeric and propagate the latest observation.
    real_financial_df = aligned_df[financial_columns].apply(pd.to_numeric, errors="coerce")
    aligned_df[financial_columns] = real_financial_df.ffill()

    # Build the weekly close-price series used to update market-based ratios
    # between quarterly statement dates.
    weekly_close_price = (
        price_df[["WeekEndingFriday", "ClosePrice"]]
        .drop_duplicates(subset=["WeekEndingFriday"], keep="last")
        .set_index("WeekEndingFriday")["ClosePrice"]
        .reindex(aligned_df.index)
    )

    # Keep accounting values fixed until the next statement, but rescale the
    # latest reported market cap with weekly price moves between quarters.
    if "marketCap" in real_financial_df.columns:
        reported_market_cap = real_financial_df["marketCap"]
    else:
        reported_market_cap = pd.Series(index=aligned_df.index, dtype="float64")
    market_cap_anchor = reported_market_cap.ffill()
    anchor_close_price = weekly_close_price.where(reported_market_cap.notna()).ffill()
    weekly_market_cap = market_cap_anchor * safe_divide(
        weekly_close_price,
        anchor_close_price,
    )

    # Derive the final weekly factors from the forward-filled accounting series.
    total_stockholders_equity = get_numeric_series(aligned_df, "totalStockholdersEquity")
    market_cap = weekly_market_cap
    gross_profit = get_numeric_series(aligned_df, "grossProfit")
    total_assets = get_numeric_series(aligned_df, "totalAssets")
    operating_income = get_numeric_series(aligned_df, "operatingIncome")
    revenue = get_numeric_series(aligned_df, "revenue")
    net_income = get_numeric_series(aligned_df, "netIncome")
    capital_expenditure = get_numeric_series(aligned_df, "capitalExpenditure")
    operating_cash_flow = get_numeric_series(aligned_df, "operatingCashFlow")
    total_debt = get_numeric_series(aligned_df, "totalDebt")
    interest_expense = get_numeric_series(aligned_df, "interestExpense")
    cash_and_cash_equivalents = get_numeric_series(
        aligned_df,
        "cashAndCashEquivalents",
    )
    total_current_liabilities = get_numeric_series(
        aligned_df,
        "totalCurrentLiabilities",
    )
    total_current_assets = get_numeric_series(aligned_df, "totalCurrentAssets")
    free_cash_flow = get_numeric_series(aligned_df, "freeCashFlow")

    derived_features_df = pd.DataFrame(index=aligned_df.index)
    derived_features_df["BookToMarket"] = safe_divide(
        total_stockholders_equity,
        market_cap,
    )
    derived_features_df["MarketCap"] = market_cap
    derived_features_df["GrossProfitability"] = safe_divide(
        gross_profit,
        total_assets,
    )

    derived_features_df["OperatingMargin"] = safe_divide(
        operating_income,
        revenue,
    )
    derived_features_df["ROA"] = safe_divide(
        net_income,
        total_assets,
    )
    derived_features_df["ROE"] = safe_divide(
        net_income,
        total_stockholders_equity,
    )
    # On the weekly aligned grid, 4 quarters are approximated with a 52-week lag.
    derived_features_df["AssetGrowth"] = safe_divide(
        total_assets - total_assets.shift(52),
        total_assets.shift(52),
    )
    derived_features_df["InvestmentIntensity"] = safe_divide(
        capital_expenditure,
        total_assets,
    )
    derived_features_df["Accruals"] = safe_divide(
        net_income - operating_cash_flow,
        total_assets,
    )
    derived_features_df["IncomeQuality"] = safe_divide(
        operating_cash_flow,
        net_income,
    )
    derived_features_df["DebtToAssets"] = safe_divide(
        total_debt,
        total_assets,
    )
    derived_features_df["InterestCoverage"] = safe_divide(
        operating_income,
        interest_expense,
    )
    derived_features_df["CashRatio"] = safe_divide(
        cash_and_cash_equivalents,
        total_current_liabilities,
    )
    derived_features_df["WorkingCapitalScaled"] = safe_divide(
        total_current_assets - total_current_liabilities,
        total_assets,
    )
    derived_features_df["FreeCashFlowYield"] = safe_divide(
        free_cash_flow,
        market_cap,
    )
    derived_features_df["EarningsYield"] = safe_divide(
        net_income,
        market_cap,
    )

    # Save only identifiers and the final engineered variables.
    aligned_df = pd.concat(
        [aligned_df[identifier_columns], derived_features_df[DERIVED_FEATURE_COLUMNS]],
        axis=1,
    )

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


# ---------------------------------------------------------------------------
# Optional Visual Check
# ---------------------------------------------------------------------------

import matplotlib.pyplot as plt

# Load a small sample of tickers for a quick visual inspection of one feature.
enterprises_df = pd.read_csv(cfg.ENT).head(10)

for ticker in enterprises_df["Ticker"]:

    # Select the company to visualize.
    PLOT_TICKER = ticker 
    # Select the financial feature to visualize.
    PLOT_FEATURE = "BookToMarket"

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
