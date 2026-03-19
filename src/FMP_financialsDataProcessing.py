#%%
import pandas as pd
import config as cfg


RAW_INPUT_FILE = cfg.FMP_RAW_FINANCIALS
OUTPUT_FILE = cfg.FMP_FINANCIALS
SINGLE_COMPANY_OUTPUT_DIR = cfg.SINGLE_COMPANY_FINANCIALS

DATE_COLUMNS = ["date", "filingDate", "acceptedDate"]


def load_raw_financials(input_file=RAW_INPUT_FILE) -> pd.DataFrame:
    if not input_file.exists():
        raise FileNotFoundError(f"Raw financials file not found: {input_file}")

    raw_df = pd.read_csv(input_file)
    if "requested_symbol" not in raw_df.columns:
        raise KeyError("The raw financials file must contain a 'requested_symbol' column.")

    for column in DATE_COLUMNS:
        if column in raw_df.columns:
            raw_df[column] = pd.to_datetime(raw_df[column], errors="coerce")

    return raw_df


def load_price_calendar(symbol: str) -> pd.DatetimeIndex:
    price_file = cfg.SINGLE_COMPANY_PRICES / f"{symbol}Prices.csv"
    if not price_file.exists():
        raise FileNotFoundError(f"Price file not found for {symbol}: {price_file}")

    price_df = pd.read_csv(price_file, parse_dates=["WeekEndingFriday"])
    if "WeekEndingFriday" not in price_df.columns:
        raise KeyError(f"'WeekEndingFriday' column not found in price file: {price_file}")

    price_index = pd.DatetimeIndex(price_df["WeekEndingFriday"]).sort_values().unique()
    price_index.name = "WeekEndingFriday"
    return price_index


def map_to_nearest_friday(date_series: pd.Series) -> pd.Series:
    parsed_dates = pd.to_datetime(date_series, errors="coerce")
    nearest_friday = pd.Series(pd.NaT, index=parsed_dates.index, dtype="datetime64[ns]")
    valid_dates = parsed_dates.notna()

    if not valid_dates.any():
        return nearest_friday

    valid_parsed_dates = parsed_dates.loc[valid_dates]
    weekday = valid_parsed_dates.dt.weekday
    days_since_prev_friday = (weekday - 4) % 7
    days_until_next_friday = (4 - weekday) % 7

    previous_friday = valid_parsed_dates - pd.to_timedelta(days_since_prev_friday, unit="D")
    next_friday = valid_parsed_dates + pd.to_timedelta(days_until_next_friday, unit="D")
    use_previous_friday = days_since_prev_friday <= days_until_next_friday

    nearest_friday.loc[valid_dates] = previous_friday.where(use_previous_friday, next_friday)
    return nearest_friday.dt.normalize()


def align_financials_to_price_calendar(company_df: pd.DataFrame, price_index: pd.DatetimeIndex) -> pd.DataFrame:
    aligned_df = pd.DataFrame(index=price_index)
    aligned_df.index.name = "WeekEndingFriday"

    working_df = company_df.copy()
    working_df["WeekEndingFriday"] = map_to_nearest_friday(working_df["date"])
    working_df = working_df.dropna(subset=["WeekEndingFriday"])

    sort_columns = [
        column
        for column in ("acceptedDate", "filingDate", "date")
        if column in working_df.columns
    ]
    if sort_columns:
        working_df = working_df.sort_values(sort_columns, ascending=False)

    working_df = (
        working_df.drop_duplicates(subset=["WeekEndingFriday"], keep="first")
        .set_index("WeekEndingFriday")
        .sort_index()
    )
    aligned_df = aligned_df.join(working_df, how="left")

    ticker = company_df["requested_symbol"].iloc[0]
    aligned_df["requested_symbol"] = ticker
    if "symbol" in aligned_df.columns:
        aligned_df["symbol"] = aligned_df["symbol"].fillna(ticker)
    else:
        aligned_df["symbol"] = ticker
    if "company_name" in aligned_df.columns:
        company_name = company_df["company_name"].dropna()
        if not company_name.empty:
            aligned_df["company_name"] = aligned_df["company_name"].fillna(company_name.iloc[0])

    leading_columns = []
    for column in ("requested_symbol", "company_name", "symbol"):
        if column in aligned_df.columns:
            leading_columns.append(column)

    remaining_columns = [
        column for column in aligned_df.columns if column not in leading_columns
    ]
    return aligned_df[leading_columns + remaining_columns]


def export_aligned_financials(raw_df: pd.DataFrame) -> pd.DataFrame:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    SINGLE_COMPANY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    combined_frames: list[pd.DataFrame] = []

    for ticker, company_df in raw_df.groupby("requested_symbol", sort=True):
        print(f"Aligning financial statements for {ticker}...")
        try:
            price_index = load_price_calendar(ticker)
        except (FileNotFoundError, KeyError) as exc:
            print(f"Price calendar error for {ticker}: {exc}")
            continue

        aligned_company_df = align_financials_to_price_calendar(company_df, price_index)
        company_output_file = SINGLE_COMPANY_OUTPUT_DIR / f"{ticker}Financials.csv"
        aligned_company_df.to_csv(company_output_file, index=True)
        print(f"Saved company file: {company_output_file}")
        combined_frames.append(aligned_company_df)

    if not combined_frames:
        return pd.DataFrame()

    combined_df = pd.concat(combined_frames).sort_index()
    combined_df.to_csv(OUTPUT_FILE, index=True)
    print(f"Saved aligned file: {OUTPUT_FILE}")
    return combined_df


def main() -> None:
    raw_df = load_raw_financials()
    aligned_df = export_aligned_financials(raw_df)

    if aligned_df.empty:
        print("Processing completed, but no aligned records were saved.")
        return

    print(
        "Processing completed: "
        f"{aligned_df['requested_symbol'].nunique()} ticker, "
        f"{len(aligned_df)} total rows."
    )


if __name__ == "__main__":
    main()


# %%
a = pd.read_csv(cfg.SINGLE_COMPANY_FINANCIALS / "AAPLFinancials.csv")
# %%
