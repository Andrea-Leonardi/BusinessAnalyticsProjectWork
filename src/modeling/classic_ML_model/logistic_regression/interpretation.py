from training_model import logistic_model, selected_variables
import pandas as pd


coefficients = logistic_model.named_steps["model"].coef_[0]

coef_df = pd.DataFrame({
    "variable": selected_variables,
    "coefficient": coefficients
})

print(coef_df.sort_values(by="coefficient", key=abs, ascending=False))  

#coefficienti molto piccoli, come previsto è presente tanto rumore