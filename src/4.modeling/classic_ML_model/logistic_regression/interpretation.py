import pandas as pd
import joblib
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import get_model_output_dir


output_dir = get_model_output_dir(Path(__file__).resolve().parent.name)
logistic_model = joblib.load(output_dir / "logistic_model.joblib")
selected_variables = pd.read_csv(
    get_model_output_dir("lasso_model") / "selected_variables.csv"
).iloc[:, 0].tolist()

coefficients = logistic_model.named_steps["model"].coef_[0]

coef_df = pd.DataFrame({
    "variable": selected_variables,
    "coefficient": coefficients
})

print(coef_df.sort_values(by="coefficient", key=abs, ascending=False))  

#coefficienti molto piccoli, come previsto è presente tanto rumore
