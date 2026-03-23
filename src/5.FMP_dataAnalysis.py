#%%
import pandas as pd
import config as cfg


DATE_COLUMN = "WeekEndingFriday"
TICKER_COLUMN = "symbol"


df = pd.read_csv(cfg.FMP_FINANCIALS, parse_dates=[DATE_COLUMN])

# Vedo che le date non sono allineate, capiamo perche'.
# Salvo in un DataFrame la prima e l'ultima riga disponibile per ogni azienda.
df_first_last = (
    df.groupby(TICKER_COLUMN)
    .agg(
        first_date=(DATE_COLUMN, "min"),
        last_date=(DATE_COLUMN, "max"),
    )
    .reset_index()
)

df_first_last["last_date"].dt.strftime("%Y-%m-%d").unique()
df_first_last["first_date"].dt.strftime("%Y-%m-%d").unique()

problematic_symbols = df_first_last.loc[
    df_first_last["last_date"] == pd.Timestamp("2019-06-28"),
    TICKER_COLUMN,
].unique()

df[df[TICKER_COLUMN].isin(problematic_symbols)]
# Quelli che finiscono il 2019-06-28 derivano da statement con data 2019-06-30,
# allineata al venerdi' piu' vicino nel file processato.


# %%
import matplotlib.pyplot as plt


SELECTED_TICKER = "BRK-B"


price_file = cfg.SINGLE_COMPANY_PRICES / f"{SELECTED_TICKER}Prices.csv"
if not price_file.exists():
    raise FileNotFoundError(f"File prezzi non trovato: {price_file}")

price_df = pd.read_csv(price_file, parse_dates=["WeekEndingFriday"])

plt.figure(figsize=(12, 6))
plt.plot(price_df["WeekEndingFriday"], price_df["AdjClosePrice"], linewidth=2)
plt.title(f"Prezzo di chiusura aggiustato di {SELECTED_TICKER}")
plt.xlabel("Data")
plt.ylabel("Adj Close Price")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# %%
