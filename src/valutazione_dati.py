import pandas as pd
import config as cfg

sec_dataset = pd.read_csv(cfg.SEC_DATASET)

print(sec_dataset.head(10))

valori_nulli = sec_dataset.isna().sum()
print(valori_nulli)

