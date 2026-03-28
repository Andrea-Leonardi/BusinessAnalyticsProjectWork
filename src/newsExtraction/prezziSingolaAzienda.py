#%%
import pandas as pd
import yfinance as yf


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Choose the ticker and the date range to download.
TICKER = "AAPL"
START_DATE = "2021-01-01"
END_DATE = "2021-12-31"


# ---------------------------------------------------------------------------
# Download Daily Prices
# ---------------------------------------------------------------------------

# Download the full daily price history for the selected company.
hist_data = yf.download(
    tickers=TICKER,
    start=START_DATE,
    end=END_DATE,
    interval="1d",
    auto_adjust=False,
    progress=False,
)

if hist_data.empty:
    raise ValueError(
        f"No daily price data was returned for {TICKER} between "
        f"{START_DATE} and {END_DATE}."
    )


# ---------------------------------------------------------------------------
# Build Pandas DataFrame
# ---------------------------------------------------------------------------

# Keep the downloaded data in a standard pandas DataFrame and expose the date
# as a normal column for easier inspection and later merges.
price_df = pd.DataFrame(hist_data).reset_index()
price_df["Ticker"] = TICKER

print(price_df.head())


# %%
