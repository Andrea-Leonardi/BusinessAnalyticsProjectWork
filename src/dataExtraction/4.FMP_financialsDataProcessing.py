#%%
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import config as cfg


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Set this to True when you want the script to re-download the raw FMP data first.
REDOWNLOAD_RAW_FMP_DATA = False

# Input/output paths used by the processing step.
ENTERPRISES_FILE = cfg.ENT
RAW_INPUT_FILE = cfg.FMP_RAW_FINANCIALS
OUTPUT_FILE = cfg.FMP_FINANCIALS
SINGLE_COMPANY_OUTPUT_DIR = cfg.SINGLE_COMPANY_FINANCIALS
FINAL_OUTPUT_START_DATE = pd.Timestamp("2021-01-01")
DATE_COLUMN = "WeekEndingFriday"
EXPECTED_COMPANY_FINANCIAL_COLUMNS = [DATE_COLUMN, "symbol"]

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

TTM_FLOW_COLUMNS = [
    "revenue",
    "grossProfit",
    "operatingIncome",
    "netIncome",
    "operatingCashFlow",
    "freeCashFlow",
]

MARKET_BASED_FEATURE_COLUMNS = [
    "BookToMarket",
    "MarketCap",
    "FreeCashFlowYield",
    "FreeCashFlowYield_TTM",
    "EarningsYield",
    "EarningsYield_TTM",
]

FUNDAMENTAL_FEATURE_COLUMNS = [
    "GrossProfitability",
    "GrossProfitability_TTM",
    "OperatingMargin",
    "OperatingMargin_TTM",
    "ROA",
    "ROA_TTM",
    "AssetGrowth",
    "InvestmentIntensity",
    "Accruals",
    "Accruals_TTM",
    "DebtToAssets",
    "WorkingCapitalScaled",
]

DERIVED_FEATURE_COLUMNS = (
    ["QuarterlyReleased"]
    + MARKET_BASED_FEATURE_COLUMNS
    + [f"{column}_L1W" for column in MARKET_BASED_FEATURE_COLUMNS]
    + [f"{column}_L2W" for column in MARKET_BASED_FEATURE_COLUMNS]
    + FUNDAMENTAL_FEATURE_COLUMNS
    + [f"{column}_L1Q" for column in FUNDAMENTAL_FEATURE_COLUMNS]
    + [f"{column}_L2Q" for column in FUNDAMENTAL_FEATURE_COLUMNS]
)


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


def load_valid_tickers() -> list[str]:
    # Leggo il perimetro aziende corrente da enterprises.csv.
    if not ENTERPRISES_FILE.exists():
        raise FileNotFoundError(f"Company file not found: {ENTERPRISES_FILE}")

    enterprises_df = pd.read_csv(ENTERPRISES_FILE, usecols=["Ticker"])
    tickers = enterprises_df["Ticker"].dropna().astype(str).str.strip()
    tickers = tickers[tickers.ne("")].drop_duplicates().tolist()
    return tickers


def company_financial_file_is_usable(file_path: Path) -> bool:
    # Considero valido solo un file finanziario aziendale leggibile e coerente.
    if not file_path.exists():
        return False

    try:
        company_df = pd.read_csv(file_path, nrows=5)
    except Exception:
        return False

    missing_columns = [
        column for column in EXPECTED_COMPANY_FINANCIAL_COLUMNS if column not in company_df.columns
    ]
    return not missing_columns


def rebuild_combined_financial_output(valid_tickers: list[str]) -> pd.DataFrame:
    # Ricostruisco financialsData.csv dai file aziendali validi, cosi l'output
    # aggregato resta completo anche quando in questo run processo solo pochi ticker.
    combined_frames = []

    for ticker in valid_tickers:
        company_output_file = SINGLE_COMPANY_OUTPUT_DIR / f"{ticker}Financials.csv"
        if not company_financial_file_is_usable(company_output_file):
            continue

        company_df = pd.read_csv(company_output_file, parse_dates=[DATE_COLUMN])
        combined_frames.append(company_df)

    if not combined_frames:
        return pd.DataFrame()

    combined_df = pd.concat(combined_frames, ignore_index=True, sort=False)
    combined_df = combined_df.sort_values(["symbol", DATE_COLUMN]).reset_index(drop=True)
    combined_df.to_csv(OUTPUT_FILE, index=False)
    return combined_df


# ---------------------------------------------------------------------------
# Optional Raw FMP Download
# ---------------------------------------------------------------------------

# Re-run the raw download script before processing when explicitly requested.
if REDOWNLOAD_RAW_FMP_DATA:
    subprocess.run(
        [sys.executable, str(cfg.DATA_EXTRACTION_SRC / "3.FMP_financialsDataGathering.py")],
        check=True,
    )


# ---------------------------------------------------------------------------
# Load Raw FMP Data
# ---------------------------------------------------------------------------

# Load the valid ticker universe first so every downstream step stays aligned.
valid_tickers = load_valid_tickers()
valid_ticker_set = set(valid_tickers)

# The raw file is useful for building missing company financial files, but the
# script can still rebuild the aggregated output from existing company files
# even when no new raw download is available.
if RAW_INPUT_FILE.exists():
    raw_df = pd.read_csv(RAW_INPUT_FILE)

    if not raw_df.empty and "requested_symbol" not in raw_df.columns:
        raise KeyError("The raw financials file must contain a 'requested_symbol' column.")

    if "requested_symbol" in raw_df.columns:
        raw_df["requested_symbol"] = raw_df["requested_symbol"].astype(str).str.strip().str.upper()
        raw_df = raw_df[raw_df["requested_symbol"].isin(valid_ticker_set)].copy()

    for column in DATE_COLUMNS:
        if column in raw_df.columns:
            raw_df[column] = pd.to_datetime(raw_df[column], errors="coerce")
else:
    raw_df = pd.DataFrame(columns=["requested_symbol"])


# ---------------------------------------------------------------------------
# Prepare Output Folders
# ---------------------------------------------------------------------------

# Ensure the output folders exist before exporting any processed files.
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
SINGLE_COMPANY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Process Each Company
# ---------------------------------------------------------------------------

# Process one ticker at a time, but skip the companies that already have a
# processed Financials.csv file.
processed_tickers: list[str] = []

for ticker, company_df in raw_df.groupby("requested_symbol", sort=True):
    company_output_file = SINGLE_COMPANY_OUTPUT_DIR / f"{ticker}Financials.csv"

    if company_financial_file_is_usable(company_output_file):
        print(f"Processed financial file already available for {ticker}, skipping.")
        continue

    print(f"Aligning financial statements for {ticker}")

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
    # Use the first date on which the market could reasonably know the new
    # accounting information. The preferred reference is acceptedDate, then
    # filingDate. When both are missing or suspiciously earlier than the
    # quarter-end statement date, fall back to the latest available date
    # among the raw date fields.
    statement_dates = pd.to_datetime(working_df["date"], errors="coerce").dt.normalize()
    if "filingDate" in working_df.columns:
        filing_dates = pd.to_datetime(working_df["filingDate"], errors="coerce").dt.normalize()
    else:
        filing_dates = pd.Series(pd.NaT, index=working_df.index, dtype="datetime64[ns]")
    if "acceptedDate" in working_df.columns:
        accepted_dates = pd.to_datetime(
            working_df["acceptedDate"],
            errors="coerce",
        ).dt.normalize()
    else:
        accepted_dates = pd.Series(
            pd.NaT,
            index=working_df.index,
            dtype="datetime64[ns]",
        )

    latest_known_date = pd.concat(
        [
            statement_dates.rename("date"),
            filing_dates.rename("filingDate"),
            accepted_dates.rename("acceptedDate"),
        ],
        axis=1,
    ).max(axis=1)

    public_dates = accepted_dates.copy()
    invalid_public_dates = public_dates.isna() | (public_dates < statement_dates)
    public_dates = public_dates.where(~invalid_public_dates, filing_dates)
    invalid_public_dates = public_dates.isna() | (public_dates < statement_dates)
    public_dates = public_dates.where(~invalid_public_dates, latest_known_date)
    valid_dates = public_dates.notna()

    # Initialize the aligned weekly date column.
    working_df["WeekEndingFriday"] = pd.NaT
    if valid_dates.any():
        # Map each release to the first Friday on or after the public date, so
        # the feature never appears in the dataset before the market could
        # observe it.
        valid_public_dates = public_dates.loc[valid_dates]
        weekday = valid_public_dates.dt.weekday
        days_until_release_friday = (4 - weekday) % 7
        release_friday = (
            valid_public_dates
            + pd.to_timedelta(days_until_release_friday, unit="D")
        ).dt.normalize()
        working_df.loc[valid_dates, "WeekEndingFriday"] = release_friday

    # Remove rows where the source date could not be aligned.
    working_df = working_df.dropna(subset=["WeekEndingFriday"])

    # Build the quarterly factors first on true statement dates. This lets the
    # weekly mapping inherit both the plain quarterly ratios and the TTM ratios
    # from older statements that already existed before the price sample starts.
    statement_level_df = working_df.copy()
    statement_sort_columns = [
        column
        for column in ("acceptedDate", "filingDate", "date")
        if column in statement_level_df.columns
    ]
    if statement_sort_columns:
        statement_level_df = statement_level_df.sort_values(
            statement_sort_columns,
            ascending=False,
        )
    if "date" in statement_level_df.columns:
        statement_level_df = statement_level_df.drop_duplicates(
            subset=["date"],
            keep="first",
        ).sort_values("date")

    statement_numeric_columns = [
        "revenue",
        "grossProfit",
        "operatingIncome",
        "netIncome",
        "interestExpense",
        "totalAssets",
        "totalStockholdersEquity",
        "totalCurrentAssets",
        "totalCurrentLiabilities",
        "totalDebt",
        "operatingCashFlow",
        "capitalExpenditure",
        "freeCashFlow",
        "marketCap",
    ]
    for column in statement_numeric_columns:
        if column in statement_level_df.columns:
            statement_level_df[column] = pd.to_numeric(
                statement_level_df[column],
                errors="coerce",
            )

    # Standardize capital expenditures as cash outflows. FMP occasionally
    # flips the sign across quarters, which can create artificial TTM zeros
    # through cancellation.
    if "capitalExpenditure" in statement_level_df.columns:
        non_missing_capex = statement_level_df["capitalExpenditure"].notna()
        statement_level_df.loc[non_missing_capex, "capitalExpenditure"] = -statement_level_df.loc[
            non_missing_capex,
            "capitalExpenditure",
        ].abs()

    # Keep explicit flags for suspicious provider-style zeros so they can be
    # converted into missing values after the weekly mapping.
    statement_level_df["CapexReportedZeroFlag"] = get_numeric_series(
        statement_level_df,
        "capitalExpenditure",
    ).eq(0).astype("int64")
    statement_level_df["FreeCashFlowZeroFlag"] = get_numeric_series(
        statement_level_df,
        "freeCashFlow",
    ).eq(0).astype("int64")

    total_debt_zero_flag = get_numeric_series(statement_level_df, "totalDebt").eq(0)
    previous_positive_debt = get_numeric_series(statement_level_df, "totalDebt").shift(1).gt(0)
    next_positive_debt = get_numeric_series(statement_level_df, "totalDebt").shift(-1).gt(0)
    statement_level_df["SuspiciousTotalDebtZeroFlag"] = (
        total_debt_zero_flag & previous_positive_debt & next_positive_debt
    ).astype("int64")

    for column in TTM_FLOW_COLUMNS:
        ttm_column = f"{column}_TTM"
        if column not in statement_level_df.columns:
            statement_level_df[ttm_column] = pd.NA
            continue

        statement_level_df[ttm_column] = statement_level_df[column].rolling(
            4,
            min_periods=4,
        ).sum()

    statement_level_df["FreeCashFlowTTMZeroFlag"] = get_numeric_series(
        statement_level_df,
        "freeCashFlow_TTM",
    ).eq(0).astype("int64")

    total_stockholders_equity_statement = get_numeric_series(
        statement_level_df,
        "totalStockholdersEquity",
    )
    gross_profit_statement = get_numeric_series(statement_level_df, "grossProfit")
    gross_profit_ttm_statement = get_numeric_series(statement_level_df, "grossProfit_TTM")
    total_assets_statement = get_numeric_series(statement_level_df, "totalAssets")
    average_assets_statement = (
        total_assets_statement + total_assets_statement.shift(4)
    ) / 2
    average_assets_statement = average_assets_statement.mask(average_assets_statement <= 0)
    operating_income_statement = get_numeric_series(statement_level_df, "operatingIncome")
    operating_income_ttm_statement = get_numeric_series(
        statement_level_df,
        "operatingIncome_TTM",
    )
    revenue_statement = get_numeric_series(statement_level_df, "revenue")
    revenue_ttm_statement = get_numeric_series(statement_level_df, "revenue_TTM")
    net_income_statement = get_numeric_series(statement_level_df, "netIncome")
    net_income_ttm_statement = get_numeric_series(statement_level_df, "netIncome_TTM")
    capital_expenditure_statement = get_numeric_series(
        statement_level_df,
        "capitalExpenditure",
    )
    operating_cash_flow_statement = get_numeric_series(
        statement_level_df,
        "operatingCashFlow",
    )
    operating_cash_flow_ttm_statement = get_numeric_series(
        statement_level_df,
        "operatingCashFlow_TTM",
    )
    total_debt_statement = get_numeric_series(statement_level_df, "totalDebt")
    total_current_liabilities_statement = get_numeric_series(
        statement_level_df,
        "totalCurrentLiabilities",
    )
    total_current_assets_statement = get_numeric_series(
        statement_level_df,
        "totalCurrentAssets",
    )

    statement_level_df["GrossProfitability"] = safe_divide(
        gross_profit_statement,
        total_assets_statement,
    )
    statement_level_df["GrossProfitability_TTM"] = safe_divide(
        gross_profit_ttm_statement,
        average_assets_statement,
    )
    statement_level_df["OperatingMargin"] = safe_divide(
        operating_income_statement,
        revenue_statement,
    )
    statement_level_df["OperatingMargin_TTM"] = safe_divide(
        operating_income_ttm_statement,
        revenue_ttm_statement,
    )
    statement_level_df["ROA"] = safe_divide(
        net_income_statement,
        total_assets_statement,
    )
    statement_level_df["ROA_TTM"] = safe_divide(
        net_income_ttm_statement,
        average_assets_statement,
    )
    statement_level_df["AssetGrowth"] = safe_divide(
        total_assets_statement - total_assets_statement.shift(4),
        total_assets_statement.shift(4),
    )
    # Express investment intensity as a positive spending ratio so higher
    # capital expenditure corresponds to a larger feature value.
    statement_level_df["InvestmentIntensity"] = safe_divide(
        -capital_expenditure_statement,
        total_assets_statement,
    )
    statement_level_df["Accruals"] = safe_divide(
        net_income_statement - operating_cash_flow_statement,
        total_assets_statement,
    )
    statement_level_df["Accruals_TTM"] = safe_divide(
        net_income_ttm_statement - operating_cash_flow_ttm_statement,
        total_assets_statement,
    )
    statement_level_df["DebtToAssets"] = safe_divide(
        total_debt_statement,
        total_assets_statement,
    )
    statement_level_df["WorkingCapitalScaled"] = safe_divide(
        total_current_assets_statement - total_current_liabilities_statement,
        total_assets_statement,
    )

    # Apply the zero-cleaning rules on the true statement timeline before
    # creating quarterly lags, so invalid quarters do not propagate as valid
    # release-to-release lagged values.
    statement_level_df.loc[
        statement_level_df["CapexReportedZeroFlag"] == 1,
        "InvestmentIntensity",
    ] = pd.NA
    statement_level_df.loc[
        statement_level_df["SuspiciousTotalDebtZeroFlag"] == 1,
        "DebtToAssets",
    ] = pd.NA

    # Build quarterly lags on the statement timeline so pre-2021 statement
    # history can still seed the first in-sample weeks after the weekly trim.
    # Add them in one batch to avoid DataFrame fragmentation warnings.
    quarterly_lag_columns: dict[str, pd.Series] = {}
    for column in FUNDAMENTAL_FEATURE_COLUMNS:
        lag_1q = statement_level_df[column].shift(1)
        lag_2q = statement_level_df[column].shift(2)
        quarterly_lag_columns[f"{column}_L1Q"] = lag_1q
        quarterly_lag_columns[f"{column}_L2Q"] = lag_2q
        quarterly_lag_columns[f"{column}_L1QMissingFlag"] = lag_1q.isna().astype(
            "int64"
        )
        quarterly_lag_columns[f"{column}_L2QMissingFlag"] = lag_2q.isna().astype(
            "int64"
        )
    statement_level_df = pd.concat(
        [statement_level_df, pd.DataFrame(quarterly_lag_columns)],
        axis=1,
    )

    working_df = statement_level_df.copy()

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
    statement_weeks = working_df.index.unique()

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

    # Mark the weeks where a new quarterly statement enters the weekly calendar.
    aligned_df["QuarterlyReleased"] = aligned_df.index.isin(statement_weeks).astype(int)

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

    # Keep the raw reported market cap before the forward fill so the weekly
    # market-cap update can anchor only on true statement rows.
    reported_market_cap = (
        pd.to_numeric(aligned_df["marketCap"], errors="coerce")
        if "marketCap" in aligned_df.columns
        else pd.Series(index=aligned_df.index, dtype="float64")
    )

    # Identify the financial columns that must be forward-filled.
    financial_columns = [
        column for column in aligned_df.columns if column not in identifier_columns
    ]

    # Convert the raw financial fields to numeric and propagate the latest observation.
    numeric_financial_df = aligned_df[financial_columns].apply(pd.to_numeric, errors="coerce")
    aligned_df[financial_columns] = numeric_financial_df.ffill()

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
    market_cap_anchor = reported_market_cap.ffill()
    anchor_close_price = weekly_close_price.where(reported_market_cap.notna()).ffill()
    weekly_market_cap = market_cap_anchor * safe_divide(
        weekly_close_price,
        anchor_close_price,
    )

    # The non-market ratios are already calculated on statement dates. After
    # the weekly alignment they just need to be carried forward. Only the
    # market-cap-based factors are recalculated every week.
    total_stockholders_equity = get_numeric_series(aligned_df, "totalStockholdersEquity")
    market_cap = weekly_market_cap
    free_cash_flow = get_numeric_series(aligned_df, "freeCashFlow")
    free_cash_flow_ttm = get_numeric_series(aligned_df, "freeCashFlow_TTM")
    net_income = get_numeric_series(aligned_df, "netIncome")
    net_income_ttm = get_numeric_series(aligned_df, "netIncome_TTM")
    capex_zero_flag = get_numeric_series(aligned_df, "CapexReportedZeroFlag")
    free_cash_flow_zero_flag = get_numeric_series(aligned_df, "FreeCashFlowZeroFlag")
    free_cash_flow_ttm_zero_flag = get_numeric_series(
        aligned_df,
        "FreeCashFlowTTMZeroFlag",
    )
    suspicious_total_debt_zero_flag = get_numeric_series(
        aligned_df,
        "SuspiciousTotalDebtZeroFlag",
    )

    derived_features_df = pd.DataFrame(index=aligned_df.index)
    derived_features_df["QuarterlyReleased"] = get_numeric_series(
        aligned_df,
        "QuarterlyReleased",
    )
    derived_features_df["BookToMarket"] = safe_divide(
        total_stockholders_equity,
        market_cap,
    )
    derived_features_df["MarketCap"] = market_cap
    derived_features_df["GrossProfitability"] = get_numeric_series(
        aligned_df,
        "GrossProfitability",
    )
    derived_features_df["GrossProfitability_TTM"] = get_numeric_series(
        aligned_df,
        "GrossProfitability_TTM",
    )
    derived_features_df["OperatingMargin"] = get_numeric_series(
        aligned_df,
        "OperatingMargin",
    )
    derived_features_df["OperatingMargin_TTM"] = get_numeric_series(
        aligned_df,
        "OperatingMargin_TTM",
    )
    derived_features_df["ROA"] = get_numeric_series(aligned_df, "ROA")
    derived_features_df["ROA_TTM"] = get_numeric_series(aligned_df, "ROA_TTM")
    derived_features_df["AssetGrowth"] = get_numeric_series(aligned_df, "AssetGrowth")
    derived_features_df["InvestmentIntensity"] = get_numeric_series(
        aligned_df,
        "InvestmentIntensity",
    )
    derived_features_df["Accruals"] = get_numeric_series(aligned_df, "Accruals")
    derived_features_df["Accruals_TTM"] = get_numeric_series(
        aligned_df,
        "Accruals_TTM",
    )
    derived_features_df["DebtToAssets"] = get_numeric_series(
        aligned_df,
        "DebtToAssets",
    )
    derived_features_df["WorkingCapitalScaled"] = get_numeric_series(
        aligned_df,
        "WorkingCapitalScaled",
    )
    derived_features_df["FreeCashFlowYield"] = safe_divide(
        free_cash_flow,
        market_cap,
    )
    derived_features_df["FreeCashFlowYield_TTM"] = safe_divide(
        free_cash_flow_ttm,
        market_cap,
    )
    derived_features_df["EarningsYield"] = safe_divide(
        net_income,
        market_cap,
    )
    derived_features_df["EarningsYield_TTM"] = safe_divide(
        net_income_ttm,
        market_cap,
    )

    # Convert suspicious zero-value segments into missing values rather than
    # keeping them as economically meaningful zeros.
    derived_features_df.loc[capex_zero_flag == 1, "InvestmentIntensity"] = pd.NA
    derived_features_df.loc[free_cash_flow_zero_flag == 1, "FreeCashFlowYield"] = pd.NA
    derived_features_df.loc[
        free_cash_flow_ttm_zero_flag == 1,
        "FreeCashFlowYield_TTM",
    ] = pd.NA
    derived_features_df.loc[suspicious_total_debt_zero_flag == 1, "DebtToAssets"] = pd.NA

    # Market-based variables move every week, so their lags are computed on
    # the weekly aligned series rather than on quarterly statement dates.
    for column in MARKET_BASED_FEATURE_COLUMNS:
        derived_features_df[f"{column}_L1W"] = derived_features_df[column].shift(1)
        derived_features_df[f"{column}_L2W"] = derived_features_df[column].shift(2)

    # Fundamental lag columns already come from the statement timeline. After
    # the weekly forward fill, apply the lag-specific missing flags so invalid
    # release quarters stay missing instead of reusing older values.
    for column in FUNDAMENTAL_FEATURE_COLUMNS:
        lag_1q_column = f"{column}_L1Q"
        lag_2q_column = f"{column}_L2Q"
        lag_1q_missing_flag = get_numeric_series(
            aligned_df,
            f"{column}_L1QMissingFlag",
        )
        lag_2q_missing_flag = get_numeric_series(
            aligned_df,
            f"{column}_L2QMissingFlag",
        )

        derived_features_df[lag_1q_column] = get_numeric_series(aligned_df, lag_1q_column)
        derived_features_df[lag_2q_column] = get_numeric_series(aligned_df, lag_2q_column)
        derived_features_df.loc[lag_1q_missing_flag == 1, lag_1q_column] = pd.NA
        derived_features_df.loc[lag_2q_missing_flag == 1, lag_2q_column] = pd.NA

    # Save only identifiers and the final engineered variables.
    aligned_df = pd.concat(
        [aligned_df[identifier_columns], derived_features_df[DERIVED_FEATURE_COLUMNS]],
        axis=1,
    )

    # Keep the pre-2021 rows only while building lags and rolling features.
    # The final exported panel starts at the main analysis window.
    aligned_df = aligned_df[aligned_df.index >= FINAL_OUTPUT_START_DATE].copy()

    # -----------------------------------------------------------------------
    # Save Company Output
    # -----------------------------------------------------------------------

    # Save the processed company-level file.
    aligned_df.to_csv(company_output_file, index=True)
    processed_tickers.append(ticker)


# ---------------------------------------------------------------------------
# Save Final Aggregated Output
# ---------------------------------------------------------------------------

# Ricostruisco sempre l'output aggregato dai file aziendali validi.
combined_df = rebuild_combined_financial_output(valid_tickers)

if combined_df.empty:
    print("Processing completed, but no aligned financial records were available.")
else:
    print(
        "Processing completed: "
        f"{combined_df['symbol'].nunique()} ticker, "
        f"{len(combined_df)} total rows, "
        f"{len(processed_tickers)} ticker processed in this run."
    )


#%%

