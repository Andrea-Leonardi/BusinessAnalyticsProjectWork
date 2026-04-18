import json
import random
from pathlib import Path
import sys

import numpy as np
import torch
import torch.nn as nn
from sklearn.preprocessing import StandardScaler
from torch.utils.data import DataLoader, TensorDataset

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_train_full, y_train_full, get_model_output_dir


SEED = 42
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
OUTPUT_DIR = get_model_output_dir(Path(__file__).resolve().parent.name)
BEST_PARAMS_PATH = OUTPUT_DIR / "best_params.json"
MODEL_PATH = OUTPUT_DIR / "neural_network_model.pt"


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
            nn.Linear(hidden_dim_2, 2),
        )

    def forward(self, x):
        return self.model(x)


def set_seed():
    random.seed(SEED)
    np.random.seed(SEED)
    torch.manual_seed(SEED)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(SEED)


def load_best_configuration():
    with open(BEST_PARAMS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def train_and_save_model():
    set_seed()
    best_configuration = load_best_configuration()
    best_params = best_configuration["best_params"]
    best_epoch = int(best_configuration["best_epoch"])
    selected_variables = best_configuration["selected_variables"]

    X_train_selected = X_train_full[selected_variables]
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train_selected)

    X_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32)
    y_train_tensor = torch.tensor(np.asarray(y_train_full), dtype=torch.long)

    train_loader = DataLoader(
        TensorDataset(X_train_tensor, y_train_tensor),
        batch_size=best_params["batch_size"],
        shuffle=True,
    )

    model = NeuralNet(
        input_dim=X_train_tensor.shape[1],
        hidden_dim_1=best_params["hidden_dim_1"],
        hidden_dim_2=best_params["hidden_dim_2"],
        dropout=best_params["dropout"],
    ).to(DEVICE)

    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(
        model.parameters(),
        lr=best_params["learning_rate"],
        weight_decay=best_params["weight_decay"],
    )

    model.train()
    for _ in range(best_epoch):
        for batch_X, batch_y in train_loader:
            batch_X = batch_X.to(DEVICE)
            batch_y = batch_y.to(DEVICE)

            optimizer.zero_grad()
            outputs = model(batch_X)
            loss = criterion(outputs, batch_y)
            loss.backward()
            optimizer.step()

    checkpoint = {
        "model_state": model.state_dict(),
        "best_params": best_params,
        "best_epoch": best_epoch,
        "selected_variables": selected_variables,
        "scaler_mean": scaler.mean_.tolist(),
        "scaler_scale": scaler.scale_.tolist(),
        "input_dim": X_train_tensor.shape[1],
    }

    torch.save(checkpoint, MODEL_PATH)
    return checkpoint


if __name__ == "__main__":
    train_and_save_model()
    print("Neural network model saved successfully.")
