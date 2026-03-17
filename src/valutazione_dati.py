#%%
import matplotlib.pyplot as plt
import pandas as pd

import config as cfg


sec_dataset = pd.read_csv(cfg.SEC_DATASET, parse_dates=["Date"])

percentuali_missing = (sec_dataset.isna().mean() * 100).sort_values(ascending=False).round(2)
print("\nPercentuale di missing value per variabile:")
print(percentuali_missing)

numero_aziende = sec_dataset["Ticker"].nunique()
print(f"\nNumero di aziende presenti nel dataset: {numero_aziende}")


# %%
