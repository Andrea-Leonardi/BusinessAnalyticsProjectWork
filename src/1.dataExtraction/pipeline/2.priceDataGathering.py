# %%
import sys
from pathlib import Path

import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import config as cfg


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Download a longer pre-sample history so 12-week return, momentum, drawdown,
# and volatility features are already available when the main sample starts in
# early 2021.
START_DATE = "2020-09-01"
DATE_COLUMN = "WeekEndingFriday"

EXPECTED_PRICE_COLUMNS = [
    "Ticker",
    "ClosePrice",
    "ClosePrice_t-1",
    "ClosePrice_t-2",
    "ClosePrice_t+1",
    "AdjClosePrice",
    "AdjClosePrice_t-1",
    "AdjClosePrice_t-2",
    "AdjClosePrice_t+1",
    "AdjClosePrice_t+1_Up",
    "WeeklyReturn_1W",
    "WeeklyReturn_4W",
    "Momentum_12W",
    "Volatility_4W",
    "Volatility_12W",
    "Drawdown_12W",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_price_output_path(ticker: str) -> Path:
    return cfg.SINGLE_COMPANY_PRICES / f"{ticker}Prices.csv"


def load_valid_tickers() -> list[str]:
    # Carico il perimetro aziende prodotto dallo screener.
    enterprises_df = pd.read_csv(cfg.ENT, usecols=["Ticker"])
    tickers = enterprises_df["Ticker"].dropna().astype(str).str.strip()
    tickers = tickers[tickers.ne("")].drop_duplicates().tolist()
    return tickers


def price_file_is_usable(file_path: Path) -> bool:
    # Considero valido solo un file leggibile con lo schema atteso.
    if not file_path.exists():
        return False

    try:
        price_df = pd.read_csv(file_path, nrows=5)
    except Exception:
        return False

    missing_columns = [column for column in [DATE_COLUMN] + EXPECTED_PRICE_COLUMNS if column not in price_df.columns]
    return not missing_columns


def download_ticker_prices(ticker: str) -> pd.DataFrame:
    # Scarico i prezzi giornalieri e li trasformo nel calendario settimanale.
    stock = yf.Ticker(ticker)
    hist_data = stock.history(start=START_DATE, interval="1d", auto_adjust=False)
    if hist_data.empty:
        return pd.DataFrame()

    daily_close = pd.DataFrame(
        {
            "ClosePrice": hist_data["Close"],
            "AdjClosePrice": hist_data["Adj Close"],
        }
    )
    daily_close.index = daily_close.index.tz_localize(None)
    daily_close.index.name = "ActualTradingDate"
    daily_close = daily_close.reset_index()

    daily_close[DATE_COLUMN] = (
        daily_close["ActualTradingDate"]
        .dt.to_period("W-FRI")
        .dt.to_timestamp(how="end")
        .dt.normalize()
    )

    company_df = (
        daily_close.sort_values("ActualTradingDate")
        .groupby(DATE_COLUMN, as_index=False)
        .tail(1)
        .drop(columns="ActualTradingDate")
        .set_index(DATE_COLUMN)
        .sort_index()
    )

    company_df["ClosePrice_t-1"] = company_df["ClosePrice"].shift(1)
    company_df["ClosePrice_t-2"] = company_df["ClosePrice"].shift(2)
    company_df["ClosePrice_t+1"] = company_df["ClosePrice"].shift(-1)
    company_df["AdjClosePrice_t-1"] = company_df["AdjClosePrice"].shift(1)
    company_df["AdjClosePrice_t-2"] = company_df["AdjClosePrice"].shift(2)
    company_df["AdjClosePrice_t+1"] = company_df["AdjClosePrice"].shift(-1)

    weekly_return_1w = company_df["AdjClosePrice"].pct_change(1)
    company_df["WeeklyReturn_1W"] = weekly_return_1w
    company_df["WeeklyReturn_4W"] = company_df["AdjClosePrice"].pct_change(4)
    company_df["Momentum_12W"] = company_df["AdjClosePrice"].pct_change(12)
    company_df["Volatility_4W"] = weekly_return_1w.rolling(window=4).std()
    company_df["Volatility_12W"] = weekly_return_1w.rolling(window=12).std()
    rolling_max_12w = company_df["AdjClosePrice"].rolling(window=12).max()
    company_df["Drawdown_12W"] = company_df["AdjClosePrice"].divide(rolling_max_12w) - 1

    company_df["AdjClosePrice_t+1_Up"] = (
        company_df["AdjClosePrice_t+1"] > company_df["AdjClosePrice"]
    ).astype("int64")

    company_df = company_df.dropna()
    company_df["Ticker"] = ticker
    company_df = company_df[EXPECTED_PRICE_COLUMNS]

    return company_df


def rebuild_all_price_data(valid_tickers: list[str]) -> None:
    # Ricostruisco l'aggregato finale solo dai file prezzi validi gia presenti.
    company_frames = []

    for ticker in valid_tickers:
        company_output_path = build_price_output_path(ticker)
        if not price_file_is_usable(company_output_path):
            continue

        company_df = pd.read_csv(company_output_path, parse_dates=[DATE_COLUMN])
        company_frames.append(company_df)

    if company_frames:
        final_df = pd.concat(company_frames, ignore_index=True)
        final_df = final_df.sort_values(["Ticker", DATE_COLUMN]).reset_index(drop=True)
        final_df.to_csv(cfg.ALL_PRICE_DATA, index=False)
        print("All done. Final price dataset saved.")
    else:
        print("No price data was available to build the aggregated dataset.")


# ---------------------------------------------------------------------------
# Load Company Universe
# ---------------------------------------------------------------------------

valid_tickers = load_valid_tickers()
cfg.SINGLE_COMPANY_PRICES.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Download Missing Weekly Price Calendars
# ---------------------------------------------------------------------------

downloaded_tickers = []
skipped_existing_tickers = []
failed_tickers = []

for ticker in valid_tickers:
    company_output_path = build_price_output_path(ticker)

    if price_file_is_usable(company_output_path):
        print(f"Price data already available for {ticker}, skipping download.")
        skipped_existing_tickers.append(ticker)
        continue

    print(f"Download data for {ticker}...")
    company_df = download_ticker_prices(ticker)

    if company_df.empty:
        print(f"No historical price data available for {ticker}")
        failed_tickers.append(ticker)
        continue

    company_df.to_csv(company_output_path, index=True)
    downloaded_tickers.append(ticker)


# ---------------------------------------------------------------------------
# Save Aggregated Output
# ---------------------------------------------------------------------------

rebuild_all_price_data(valid_tickers)
print(
    "Price gathering completed:",
    {
        "valid_tickers": len(valid_tickers),
        "downloaded_tickers": downloaded_tickers,
        "skipped_existing_tickers": len(skipped_existing_tickers),
        "failed_tickers": failed_tickers,
        "aggregate_output": str(cfg.ALL_PRICE_DATA),
    },
)


# %%
