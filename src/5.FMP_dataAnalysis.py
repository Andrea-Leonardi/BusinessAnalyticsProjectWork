#%%
from pathlib import Path
import pandas as pd
import config as cfg


df = pd.read_csv(cfg.FMP_FINANCIALS)

#vedo che le date non sono allineate, capiamo perchè
#save in a dataframe the first and the last row for each company
df_first_last = df.groupby("symbol").agg({"date": ["first", "last"]}).reset_index()
df_first_last.columns = ["symbol", "first_date", "last_date"]   

df_first_last["last_date"].unique()
df_first_last["first_date"].unique()

df[df["symbol"].isin(df_first_last[df_first_last["last_date"] == "2019-06-30"]["symbol"].unique())]
#quelli che iniziano nel 2016 sono quelli che hanno dati semestrali non trimestrali



# %%
