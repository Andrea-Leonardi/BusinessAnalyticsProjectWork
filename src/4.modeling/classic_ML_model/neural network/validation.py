import json
import copy
import random

from pathlib import Path
import sys

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train, y_train, X_validation, y_validation


# --------------------------------------------------
# impostazioni generali
# --------------------------------------------------

SEED = 42
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

print("Device usato:", DEVICE)

OUTPUT_DIR = Path(__file__).resolve().parent

USE_SELECTED_VARIABLES = True
SELECTED_VARIABLES_PATH = OUTPUT_DIR.parent / "lasso_model" / "selected_variables.csv"

N_EPOCHS = 100
PATIENCE = 10


# --------------------------------------------------
# riproducibilità
# --------------------------------------------------

random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)
if torch.cuda.is_available():
    torch.cuda.manual_seed_all(SEED)


# --------------------------------------------------
# eventuale selezione variabili
# --------------------------------------------------

if USE_SELECTED_VARIABLES:
    selected_variables = pd.read_csv(SELECTED_VARIABLES_PATH).iloc[:, 0].tolist()
    X_train = X_train[selected_variables]
    X_validation = X_validation[selected_variables]
else:
    selected_variables = list(X_train.columns)


# --------------------------------------------------
# scaler: fit solo sul training
# --------------------------------------------------

scaler = StandardScaler()

X_train_scaled = scaler.fit_transform(X_train)
X_validation_scaled = scaler.transform(X_validation)

X_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32)
y_train_tensor = torch.tensor(np.asarray(y_train), dtype=torch.long)

X_validation_tensor = torch.tensor(X_validation_scaled, dtype=torch.float32)
y_validation_tensor = torch.tensor(np.asarray(y_validation), dtype=torch.long)

input_dim = X_train_tensor.shape[1]


# --------------------------------------------------
# dataset pytorch
# --------------------------------------------------

train_dataset = TensorDataset(X_train_tensor, y_train_tensor)


# --------------------------------------------------
# definizione rete
# --------------------------------------------------

class NeuralNet(nn.Module):
    def __init__(self, input_dim, hidden_dim_1, hidden_dim_2, dropout):
        super().__init__()

        self.model = nn.Sequential(
            nn.Linear(input_dim, hidden_dim_1),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(hidden_dim_1, hidden_dim_2),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(hidden_dim_2, 2)
        )

    def forward(self, x):
        return self.model(x)


# --------------------------------------------------
# funzione di training/validation
# --------------------------------------------------

def train_and_validate(params):
    model = NeuralNet(
        input_dim=input_dim,
        hidden_dim_1=params["hidden_dim_1"],
        hidden_dim_2=params["hidden_dim_2"],
        dropout=params["dropout"],
    ).to(DEVICE)

    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(
        model.parameters(),
        lr=params["learning_rate"],
        weight_decay=params["weight_decay"],
    )

    train_loader = DataLoader(
        train_dataset,
        batch_size=params["batch_size"],
        shuffle=True,
    )

    best_model_state = None
    best_val_loss = float("inf")
    best_epoch = 1
    patience_counter = 0

    for epoch in range(N_EPOCHS):
        # training
        model.train()
        for batch_X, batch_y in train_loader:
            batch_X = batch_X.to(DEVICE)
            batch_y = batch_y.to(DEVICE)

            optimizer.zero_grad()
            outputs = model(batch_X)
            loss = criterion(outputs, batch_y)
            loss.backward()
            optimizer.step()

        # validation
        model.eval()
        with torch.no_grad():
            val_outputs = model(X_validation_tensor.to(DEVICE))
            val_loss = criterion(val_outputs, y_validation_tensor.to(DEVICE)).item()

        # early stopping su validation loss
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_model_state = copy.deepcopy(model.state_dict())
            best_epoch = epoch + 1
            patience_counter = 0
        else:
            patience_counter += 1

        if patience_counter >= PATIENCE:
            break

    # ripristina il modello migliore visto su validation
    model.load_state_dict(best_model_state)
    model.eval()

    with torch.no_grad():
        logits = model(X_validation_tensor.to(DEVICE))
        y_pred = torch.argmax(logits, dim=1).cpu().numpy()

    val_accuracy = accuracy_score(y_validation, y_pred)

    return model, best_val_loss, val_accuracy, best_epoch


# --------------------------------------------------
# griglia iperparametri
# --------------------------------------------------

param_grid = {
    "hidden_dim_1": [16, 32, 64],
    "hidden_dim_2": [8, 16, 32],
    "dropout": [0.0, 0.2],
    "learning_rate": [1e-4, 5e-4, 1e-3],
    "weight_decay": [0.0, 1e-5, 1e-4],
    "batch_size": [32, 64],
}


# --------------------------------------------------
# loop validation
# --------------------------------------------------

best_score = -1
best_params = None
best_model = None
best_epoch_overall = 1
scores = {}

for hidden_dim_1 in param_grid["hidden_dim_1"]:
    for hidden_dim_2 in param_grid["hidden_dim_2"]:
        for dropout in param_grid["dropout"]:
            for learning_rate in param_grid["learning_rate"]:
                for weight_decay in param_grid["weight_decay"]:
                    for batch_size in param_grid["batch_size"]:

                        params = {
                            "hidden_dim_1": hidden_dim_1,
                            "hidden_dim_2": hidden_dim_2,
                            "dropout": dropout,
                            "learning_rate": learning_rate,
                            "weight_decay": weight_decay,
                            "batch_size": batch_size,
                        }

                        model, val_loss, val_accuracy, best_epoch = train_and_validate(params)

                        scores[str(params)] = {
                            "validation_accuracy": val_accuracy,
                            "validation_loss": val_loss,
                            "best_epoch": best_epoch,
                        }

                        print(
                            f"Params: {params} | "
                            f"Validation accuracy: {val_accuracy:.6f} | "
                            f"Validation loss: {val_loss:.6f}"
                        )

                        if val_accuracy > best_score:
                            best_score = val_accuracy
                            best_params = params
                            best_model = copy.deepcopy(model.state_dict())
                            best_epoch_overall = best_epoch


# --------------------------------------------------
# salvataggio risultati
# --------------------------------------------------

with open(OUTPUT_DIR / "best_params.json", "w") as f:
    json.dump(
        {
            "best_params": best_params,
            "best_score": best_score,
            "metric": "accuracy",
            "scores": scores,
            "selected_variables": selected_variables,
            "input_dim": input_dim,
            "best_epoch": best_epoch_overall,
        },
        f,
        indent=4
    )

torch.save(best_model, OUTPUT_DIR / "best_model_state.pt")

print("\nBest params:", best_params)
print("Best validation accuracy:", best_score)
print("Risultati salvati correttamente.")

"""
"""
