import pandas as pd
import joblib
from pathlib import Path

current_dir = Path(__file__).resolve().parent
logistic_model = joblib.load(current_dir / "logistic_model.joblib")
selected_variables = pd.read_csv(
    current_dir.parent / "lasso_model" / "selected_variables.csv"
).iloc[:, 0].tolist()

coefficients = logistic_model.named_steps["model"].coef_[0]

coef_df = pd.DataFrame({
    "variable": selected_variables,
    "coefficient": coefficients
})

print(coef_df.sort_values(by="coefficient", key=abs, ascending=False))  

#coefficienti molto piccoli, come previsto è presente tanto rumore
