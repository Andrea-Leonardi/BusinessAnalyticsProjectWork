import pandas as pd

sec_dataset = pd.read_csv("sec_dataset.csv")

print(sec_dataset.head(1))

valori_nulli = sec_dataset.isna().sum()
print(valori_nulli)

