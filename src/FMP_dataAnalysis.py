#%%
from pathlib import Path
import pandas as pd
import config as cfg


df = pd.read_csv(cfg.FMP_ALL_COMP)

#stampo lista nomi colonne df
print(df.columns)
# %%
