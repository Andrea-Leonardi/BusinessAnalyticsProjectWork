from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lasso_model.variable_selection import selected_variables

variables_list = selected_variables.iloc[:, 0].tolist()

print(variables_list)


#addattare il training, validation, test set alle variabili selezionate (da fare!!!)