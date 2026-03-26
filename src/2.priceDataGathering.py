# %%
import yfinance as yf
import pandas as pd
import config as cfg


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Download a short pre-sample history so weekly lags are already available
# when the main analysis window starts in 2021.
START_DATE = "2020-12-01"


# ---------------------------------------------------------------------------
# Load Company Universe
# ---------------------------------------------------------------------------

# Load the list of selected companies produced by the screener step.
df = pd.read_csv(cfg.ENT)

# Collect the weekly price DataFrames for all companies before saving the
# aggregated file at the end of the script.
company_dfs = {}
cfg.SINGLE_COMPANY_PRICES.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Download Weekly Price Calendars
# ---------------------------------------------------------------------------

for ticker in df["Ticker"]:
    print(f"Download data for {ticker}...")
    stock = yf.Ticker(ticker)

    # Download unadjusted daily prices and keep the adjusted close as a
    # secondary field for future analyses when needed.
    hist_data = stock.history(start=START_DATE, interval="1d", auto_adjust=False)
    if hist_data.empty:
        print(f"No historical price data available for {ticker}")
        continue

    # Keep both close series so the project can use either market prices
    # or adjusted prices depending on the downstream task.
    daily_close = pd.DataFrame(
        {
            "ClosePrice": hist_data["Close"],
            "AdjClosePrice": hist_data["Adj Close"],
        }
    )
    daily_close.index = daily_close.index.tz_localize(None)
    daily_close.index.name = "ActualTradingDate"
    daily_close = daily_close.reset_index()

    # Map every trading day to its corresponding week-ending Friday.
    daily_close["WeekEndingFriday"] = (
        daily_close["ActualTradingDate"]
        .dt.to_period("W-FRI")
        .dt.to_timestamp(how="end")
        .dt.normalize()
    )

    # Keep the last available trading day inside each weekly bucket.
    company_df = (
        daily_close.sort_values("ActualTradingDate")
        .groupby("WeekEndingFriday", as_index=False)
        .tail(1)
        .drop(columns="ActualTradingDate")
        .set_index("WeekEndingFriday")
        .sort_index()
    )

    # Add short lags that can be reused in later analyses.
    company_df["ClosePrice_t-1"] = company_df["ClosePrice"].shift(1)
    company_df["ClosePrice_t-2"] = company_df["ClosePrice"].shift(2)
    company_df["ClosePrice_t+1"] = company_df["ClosePrice"].shift(-1)
    company_df["AdjClosePrice_t-1"] = company_df["AdjClosePrice"].shift(1)
    company_df["AdjClosePrice_t-2"] = company_df["AdjClosePrice"].shift(2)
    company_df["AdjClosePrice_t+1"] = company_df["AdjClosePrice"].shift(-1)
    # Use a simple binary target: 1 when next week's adjusted close is higher
    # than the current adjusted close, 0 otherwise.
    company_df["AdjClosePrice_t+1_Up"] = (
        company_df["AdjClosePrice_t+1"] > company_df["AdjClosePrice"]
    ).astype("int64")

    # Drop the edge rows where lagged or forward values are still unavailable.
    company_df = company_df.dropna()
    company_df["Ticker"] = ticker
    company_df = company_df[
        [
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
        ]
    ]

    # Save the company-level weekly price file.
    company_dfs[ticker] = company_df
    company_output_path = cfg.SINGLE_COMPANY_PRICES / f"{ticker}Prices.csv"
    company_df.to_csv(company_output_path, index=True)


# ---------------------------------------------------------------------------
# Save Aggregated Output
# ---------------------------------------------------------------------------

# Merge all company-level price files into one final dataset.
if company_dfs:
    final_df = pd.concat(company_dfs.values()).sort_index()
    final_df.to_csv(cfg.ALL_PRICE_DATA, index=True)
    print("All done. Final dataset saved")
else:
    print("No price data was downloaded.")


# %%
