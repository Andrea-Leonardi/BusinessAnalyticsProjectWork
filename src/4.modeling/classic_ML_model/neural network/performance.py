import json
from pathlib import Path
import sys

import numpy as np
import torch
import torch.nn as nn
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test


DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
INPUT_DIR = Path(__file__).resolve().parent
MODEL_PATH = INPUT_DIR / "neural_network_model.pt"
BEST_PARAMS_PATH = INPUT_DIR / "best_params.json"
PERFORMANCE_PATH = INPUT_DIR / "performance.json"


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


def evaluate_and_save_performance():
    checkpoint = torch.load(MODEL_PATH, map_location=DEVICE)
    best_params = checkpoint["best_params"]
    selected_variables = checkpoint["selected_variables"]

    with open(BEST_PARAMS_PATH, "r", encoding="utf-8") as f:
        validation_results = json.load(f)

    scaler = StandardScaler()
    scaler.mean_ = np.asarray(checkpoint["scaler_mean"], dtype=np.float64)
    scaler.scale_ = np.asarray(checkpoint["scaler_scale"], dtype=np.float64)
    scaler.var_ = scaler.scale_ ** 2
    scaler.n_features_in_ = len(selected_variables)

    X_test_selected = X_test[selected_variables]
    X_test_scaled = scaler.transform(X_test_selected)
    X_test_tensor = torch.tensor(X_test_scaled, dtype=torch.float32).to(DEVICE)

    model = NeuralNet(
        input_dim=checkpoint["input_dim"],
        hidden_dim_1=best_params["hidden_dim_1"],
        hidden_dim_2=best_params["hidden_dim_2"],
        dropout=best_params["dropout"],
    ).to(DEVICE)
    model.load_state_dict(checkpoint["model_state"])
    model.eval()

    with torch.no_grad():
        logits = model(X_test_tensor)
        y_pred_test = torch.argmax(logits, dim=1).cpu().numpy()

    test_accuracy = float(accuracy_score(y_test, y_pred_test))
    performance = {
        "model": "neural_network",
        "metric": "accuracy",
        "validation_accuracy": float(validation_results["best_score"]),
        "best_params": validation_results["best_params"],
        "best_epoch": int(checkpoint["best_epoch"]),
        "selected_variables": len(selected_variables),
        "test_accuracy": test_accuracy,
    }

    with open(PERFORMANCE_PATH, "w", encoding="utf-8") as f:
        json.dump(performance, f, indent=4)

    print("Neural network test accuracy:", test_accuracy)
    return performance


if __name__ == "__main__":
    evaluate_and_save_performance()
