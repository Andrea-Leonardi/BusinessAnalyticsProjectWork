# %%
import yfinance as yf
import pandas as pd
import config as cfg

START_DATE = "2021-01-01"

df = pd.read_csv(cfg.ENT)

# create a dictionary to store the dataframes for each company
company_dfs = {}
cfg.SINGLE_COMPANY_PRICES.mkdir(parents=True, exist_ok=True)

for ticker in df["Ticker"]:
    print(f"download data for {ticker}...")
    stock = yf.Ticker(ticker)

    # Download daily prices and keep the last available close for each week.
    hist_data = stock.history(start=START_DATE, interval='1d', auto_adjust=True)
    if hist_data.empty:
        print(f"No historical price data available for {ticker}")
        continue

    daily_close = pd.DataFrame({"AdjClosePrice": hist_data["Close"]})
    daily_close.index = daily_close.index.tz_localize(None)
    daily_close.index.name = "ActualTradingDate"
    daily_close = daily_close.reset_index()
    daily_close["WeekEndingFriday"] = (
        daily_close["ActualTradingDate"]
        .dt.to_period("W-FRI")
        .dt.to_timestamp(how="end")
        .dt.normalize()
    )

    company_df = (
        daily_close.sort_values("ActualTradingDate")
        .groupby("WeekEndingFriday", as_index=False)
        .tail(1)
        .drop(columns="ActualTradingDate")
        .set_index("WeekEndingFriday")
        .sort_index()
    )
    company_df["AdjClosePrice_t-1"] = company_df["AdjClosePrice"].shift(1)
    company_df["AdjClosePrice_t-2"] = company_df["AdjClosePrice"].shift(2)
    company_df = company_df.dropna()
    company_df["Ticker"] = ticker
    company_df = company_df[
        ["Ticker", "AdjClosePrice", "AdjClosePrice_t-1", "AdjClosePrice_t-2"]
    ]
    company_dfs[ticker] = company_df
    company_output_path = cfg.SINGLE_COMPANY_PRICES / f"{ticker}Prices.csv"
    company_df.to_csv(company_output_path, index=True)
    print(f"Saved file: {company_output_path}")

if company_dfs:
    final_df = pd.concat(company_dfs.values()).sort_index()
    final_df.to_csv(cfg.ALL_PRICE_DATA, index=True)
    print(f"Saved file: {cfg.ALL_PRICE_DATA}")
else:
    print("No price data was downloaded.")


# %%
