import numpy as np
from pathlib import Path
import sys

from sklearn.dummy import DummyClassifier


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full, y_train_full



# modello nullo
null_model = DummyClassifier(
    strategy="most_frequent"
)


# addestramento
null_model.fit(X_train_full, y_train_full)
