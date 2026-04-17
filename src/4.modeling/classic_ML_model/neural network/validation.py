import json
import copy
import os
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
import optuna
from optuna.pruners import MedianPruner
from optuna.samplers import TPESampler


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train, y_train, X_validation, y_validation


# --------------------------------------------------
# impostazioni generali
# --------------------------------------------------

SEED = 42
REQUESTED_DEVICE = os.getenv("TORCH_DEVICE", "auto").strip().lower()


def resolve_device():
    if REQUESTED_DEVICE not in {"auto", "cpu", "cuda"}:
        raise ValueError(
            "TORCH_DEVICE deve essere uno tra 'auto', 'cpu' o 'cuda'. "
            f"Valore ricevuto: {REQUESTED_DEVICE!r}"
        )

    if REQUESTED_DEVICE == "cpu":
        return torch.device("cpu")

    if REQUESTED_DEVICE == "cuda":
        if not torch.cuda.is_available():
            raise RuntimeError(
                "TORCH_DEVICE='cuda' ma CUDA non e disponibile in PyTorch. "
                f"torch={torch.__version__}, torch.version.cuda={torch.version.cuda}. "
                "Installa una build CUDA di PyTorch."
            )
        return torch.device("cuda")

    if torch.cuda.is_available():
        return torch.device("cuda")

    return torch.device("cpu")


DEVICE = resolve_device()

print("Device usato:", DEVICE)
if DEVICE.type == "cuda":
    print("GPU:", torch.cuda.get_device_name(0))
else:
    print(
        "CUDA non disponibile in PyTorch. "
        f"torch={torch.__version__}, torch.version.cuda={torch.version.cuda}"
    )

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

X_validation_tensor = X_validation_tensor.to(DEVICE)
y_validation_tensor = y_validation_tensor.to(DEVICE)

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
        pin_memory=DEVICE.type == "cuda",
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
            val_outputs = model(X_validation_tensor)
            val_loss = criterion(val_outputs, y_validation_tensor).item()

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
        logits = model(X_validation_tensor)
        y_pred = torch.argmax(logits, dim=1).cpu().numpy()

    val_accuracy = accuracy_score(y_validation, y_pred)

    return model, best_val_loss, val_accuracy, best_epoch


# --------------------------------------------------
# ottimizzazione con Optuna
# --------------------------------------------------

def objective(trial):
    """Funzione obiettivo per Optuna"""
    # Definisci lo spazio di ricerca (hyperparametri da ottimizzare)
    hidden_dim_1 = trial.suggest_int("hidden_dim_1", 16, 128, step=16)
    hidden_dim_2 = trial.suggest_int("hidden_dim_2", 8, 64, step=8)
    dropout = trial.suggest_float("dropout", 0.0, 0.5, step=0.05)
    learning_rate = trial.suggest_float("learning_rate", 1e-5, 1e-2, log=True)
    weight_decay = trial.suggest_float("weight_decay", 1e-6, 1e-3, log=True)
    batch_size = trial.suggest_categorical("batch_size", [16, 32, 64, 128])

    params = {
        "hidden_dim_1": hidden_dim_1,
        "hidden_dim_2": hidden_dim_2,
        "dropout": dropout,
        "learning_rate": learning_rate,
        "weight_decay": weight_decay,
        "batch_size": batch_size,
    }

    try:
        model, val_loss, val_accuracy, best_epoch = train_and_validate(params)
        
        print(
            f"Trial {trial.number}: "
            f"hidden1={hidden_dim_1}, hidden2={hidden_dim_2}, "
            f"dropout={dropout:.3f}, lr={learning_rate:.2e}, "
            f"wd={weight_decay:.2e}, bs={batch_size} | "
            f"Accuracy: {val_accuracy:.6f}, Loss: {val_loss:.6f}"
        )
        
        return val_accuracy
    except Exception as e:
        print(f"Errore nel trial {trial.number}: {e}")
        return 0.0


# Crea uno studio Optuna con TPE sampler e pruner
sampler = TPESampler(seed=SEED)
pruner = MedianPruner()

study = optuna.create_study(
    direction="maximize",
    sampler=sampler,
    pruner=pruner,
)

# Ottimizzazione
n_trials = 100  # Numero di trials da eseguire
print(f"Inizio ottimizzazione con Optuna ({n_trials} trials)...")
study.optimize(objective, n_trials=n_trials, show_progress_bar=True)

# Estrai il miglior trial
best_trial = study.best_trial
best_params = best_trial.params
best_score = best_trial.value

# Ricrea il miglior modello
model, _, _, best_epoch_overall = train_and_validate(best_params)
best_model = copy.deepcopy(model.state_dict())

# Raccogli statistiche
scores = {}
for trial in study.trials:
    scores[str(trial.params)] = {
        "validation_accuracy": trial.value,
        "validation_loss": None,  # Non salvato durante Optuna
        "best_epoch": None,
    }


# --------------------------------------------------
# salvataggio risultati
# --------------------------------------------------

with open(OUTPUT_DIR / "best_params.json", "w") as f:
    json.dump(
        {
            "best_params": best_params,
            "best_score": best_score,
            "metric": "accuracy",
            "n_trials": n_trials,
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
