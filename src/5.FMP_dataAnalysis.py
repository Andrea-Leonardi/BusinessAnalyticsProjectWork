#%%
import pandas as pd
import config as cfg


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Define the key columns used to inspect the processed financial dataset.
DATE_COLUMN = "WeekEndingFriday"
TICKER_COLUMN = "symbol"


# ---------------------------------------------------------------------------
# Load Processed Financial Dataset
# ---------------------------------------------------------------------------

# Load the final processed financial dataset created by the data processing step.
df = pd.read_csv(cfg.FMP_FINANCIALS, parse_dates=[DATE_COLUMN])
dfRaw = pd.read_csv(cfg.FMP_RAW_FINANCIALS)



# ---------------------------------------------------------------------------
# Inspect Date Coverage By Company
# ---------------------------------------------------------------------------

# Summarize the first and last available date for each company so it is easy
# to detect alignment issues or truncated histories.
df_first_last = (
    df.groupby(TICKER_COLUMN)
    .agg(
        first_date=(DATE_COLUMN, "min"),
        last_date=(DATE_COLUMN, "max"),
    )
    .reset_index()
)

# Inspect the unique date ranges visible in the processed dataset.
df_first_last["last_date"].dt.strftime("%Y-%m-%d").unique()
df_first_last["first_date"].dt.strftime("%Y-%m-%d").unique()

# Identify companies whose history ends on a suspicious boundary date.
problematic_symbols = df_first_last.loc[
    df_first_last["last_date"] == pd.Timestamp("2019-06-28"),
    TICKER_COLUMN,
].unique()

# Display the rows for the suspicious companies to inspect the alignment.
df[df[TICKER_COLUMN].isin(problematic_symbols)]

# The companies that end on 2019-06-28 come from statements dated 2019-06-30,
# which are aligned to the closest Friday in the processed weekly dataset.


# %%
import matplotlib.pyplot as plt


# ---------------------------------------------------------------------------
# Plot Weekly Price Example
# ---------------------------------------------------------------------------

# Select one company to inspect the weekly price series visually.
SELECTED_TICKER = "BRK-B"

# Load the company-level weekly price file associated with the selected ticker.
price_file = cfg.SINGLE_COMPANY_PRICES / f"{SELECTED_TICKER}Prices.csv"
if not price_file.exists():
    raise FileNotFoundError(f"Price file not found: {price_file}")

price_df = pd.read_csv(price_file, parse_dates=["WeekEndingFriday"])

# Plot the adjusted close price to check the weekly price calendar visually.
plt.figure(figsize=(12, 6))
plt.plot(price_df["WeekEndingFriday"], price_df["AdjClosePrice"], linewidth=2)
plt.title(f"Adjusted Close Price for {SELECTED_TICKER}")
plt.xlabel("Date")
plt.ylabel("Adj Close Price")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# %%
