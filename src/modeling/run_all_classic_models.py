from pathlib import Path
import sys
import json
import copy
import random

import joblib
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[1]
MODELING_DIR = Path(__file__).resolve().parent
CLASSIC_DIR = MODELING_DIR / "classic_ML_model"
LASSO_DIR = CLASSIC_DIR / "lasso_model"
LOGIT_DIR = CLASSIC_DIR / "logistic_regression"
NULL_DIR = CLASSIC_DIR / "null_model"
RF_DIR = CLASSIC_DIR / "random_forest"
XGB_DIR = CLASSIC_DIR / "XGBoost"
NN_DIR = CLASSIC_DIR / "neural network"

sys.path.insert(0, str(CLASSIC_DIR))
import split_data


RANDOM_SEED = 42
TOP_K_VARIABLES = None
RUN_NEURAL_NETWORK = True

LASSO_C_GRID = [
    1,
    0.3, 0.1, 0.05, 0.03, 0.02, 0.015, 0.01,
    0.007, 0.005, 0.003, 0.002, 0.001,
    0.0007, 0.0005, 0.0003, 0.0001,
]

RF_PARAM_GRID = {
    "n_estimators": [200, 300],
    "max_depth": [3, 4, 5],
    "min_samples_leaf": [10, 20, 30],
    "max_features": ["sqrt"],
}

XGB_PARAM_GRID = {
    "n_estimators": [50, 75, 100],
    "learning_rate": [0.004, 0.005, 0.006, 0.007],
    "max_depth": [5, 6, 7, 8, 9],
    "min_child_weight": [4, 5, 6, 7],
    "subsample": [0.1, 0.2, 0.3, 0.4, 0.5],
    "colsample_bytree": [0.7, 0.8, 0.9, 1.0],
}

NN_PARAM_GRID = {
    "hidden_dim_1": [16, 32, 64],
    "hidden_dim_2": [8, 16, 32],
    "dropout": [0.0, 0.2],
    "learning_rate": [1e-4, 5e-4, 1e-3],
    "weight_decay": [0.0, 1e-5, 1e-4],
    "batch_size": [32, 64],
}
NN_EPOCHS = 100
NN_PATIENCE = 10


def ensure_directories() -> None:
    for directory in [LASSO_DIR, LOGIT_DIR, NULL_DIR, RF_DIR, XGB_DIR, NN_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def save_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=4), encoding="utf-8")


def build_lasso_model(C: float) -> Pipeline:
    return Pipeline([
        ("scaler", StandardScaler()),
        ("model", LogisticRegression(
            l1_ratio=1.0,
            C=C,
            solver="saga",
            max_iter=7000,
            random_state=RANDOM_SEED,
        )),
    ])


def select_variables_from_lasso_model(lasso_model: Pipeline) -> pd.DataFrame:
    model = lasso_model.named_steps["model"]
    coefs = model.coef_.flatten()
    feature_names = getattr(lasso_model, "feature_names_in_", split_data.X_train_full.columns)

    results = pd.DataFrame({
        "variable": feature_names,
        "coefficient": coefs,
    })
    results["abs_coefficient"] = results["coefficient"].abs()
    results = results.sort_values("abs_coefficient", ascending=False).reset_index(drop=True)

    if TOP_K_VARIABLES is None:
        return results[results["coefficient"] != 0].copy()

    if TOP_K_VARIABLES <= 0:
        raise ValueError("TOP_K_VARIABLES must be a positive integer or None.")

    return results.head(TOP_K_VARIABLES).copy()


def run_null_model() -> dict:
    null_model = DummyClassifier(strategy="most_frequent")
    null_model.fit(split_data.X_train_full, split_data.y_train_full)
    joblib.dump(null_model, NULL_DIR / "null_model.joblib")

    y_pred_test = null_model.predict(split_data.X_test)
    predicted_class = int(y_pred_test[0]) if len(y_pred_test) > 0 else None
    test_accuracy = accuracy_score(split_data.y_test, y_pred_test)

    return {
        "model": "null_model",
        "predicted_class": predicted_class,
        "test_accuracy": test_accuracy,
    }


def run_lasso_pipeline() -> tuple[dict, list[str]]:
    best_score = -1.0
    best_C = None
    best_nonzero_count = None
    scores = {}

    for C in LASSO_C_GRID:
        lasso_model = build_lasso_model(C)
        lasso_model.fit(split_data.X_train, split_data.y_train)

        y_pred_validation = lasso_model.predict(split_data.X_validation)
        score = accuracy_score(split_data.y_validation, y_pred_validation)
        nonzero_count = int((lasso_model.named_steps["model"].coef_.ravel() != 0).sum())
        scores[C] = score

        if score > best_score or (score == best_score and (best_C is None or C > best_C)):
            best_score = score
            best_C = C
            best_nonzero_count = nonzero_count

    save_json(
        LASSO_DIR / "best_C.json",
        {
            "best_C": best_C,
            "best_score": best_score,
            "best_nonzero_count": best_nonzero_count,
            "metric": "accuracy",
            "scores": scores,
        },
    )

    final_lasso_model = build_lasso_model(best_C)
    final_lasso_model.fit(split_data.X_train_full, split_data.y_train_full)
    joblib.dump(final_lasso_model, LASSO_DIR / "lasso_logistic_model.pkl")

    selected_variables_df = select_variables_from_lasso_model(final_lasso_model)
    selected_variables_df[["variable"]].to_csv(LASSO_DIR / "selected_variables.csv", index=False)
    selected_variables = selected_variables_df["variable"].tolist()

    y_pred_test = final_lasso_model.predict(split_data.X_test)
    test_accuracy = accuracy_score(split_data.y_test, y_pred_test)

    return (
        {
            "model": "lasso_logistic",
            "validation_accuracy": best_score,
            "best_C": best_C,
            "selected_variables": len(selected_variables),
            "test_accuracy": test_accuracy,
        },
        selected_variables,
    )


def run_logistic_pipeline(selected_variables: list[str]) -> dict:
    logistic_model = Pipeline([
        ("scaler", StandardScaler()),
        ("model", LogisticRegression(
            max_iter=5000,
            random_state=RANDOM_SEED,
        )),
    ])

    X_train_selected_full = split_data.X_train_full[selected_variables]
    X_test_selected = split_data.X_test[selected_variables]

    logistic_model.fit(X_train_selected_full, split_data.y_train_full)
    joblib.dump(logistic_model, LOGIT_DIR / "logistic_model.joblib")

    y_pred_test = logistic_model.predict(X_test_selected)
    test_accuracy = accuracy_score(split_data.y_test, y_pred_test)

    return {
        "model": "logistic_regression",
        "selected_variables": len(selected_variables),
        "test_accuracy": test_accuracy,
    }


def run_random_forest_pipeline(selected_variables: list[str]) -> dict:
    X_train_selected = split_data.X_train[selected_variables]
    X_validation_selected = split_data.X_validation[selected_variables]

    best_score = -1.0
    best_params = None
    scores = {}

    for n_estimators in RF_PARAM_GRID["n_estimators"]:
        for max_depth in RF_PARAM_GRID["max_depth"]:
            for min_samples_leaf in RF_PARAM_GRID["min_samples_leaf"]:
                for max_features in RF_PARAM_GRID["max_features"]:
                    params = {
                        "n_estimators": n_estimators,
                        "max_depth": max_depth,
                        "min_samples_leaf": min_samples_leaf,
                        "max_features": max_features,
                    }
                    random_forest_model = RandomForestClassifier(
                        **params,
                        random_state=RANDOM_SEED,
                        n_jobs=1,
                    )
                    random_forest_model.fit(X_train_selected, split_data.y_train)

                    y_pred_validation = random_forest_model.predict(X_validation_selected)
                    score = accuracy_score(split_data.y_validation, y_pred_validation)
                    scores[str(params)] = score

                    if score > best_score:
                        best_score = score
                        best_params = params

    save_json(
        RF_DIR / "best_params.json",
        {
            "best_params": best_params,
            "best_score": best_score,
            "metric": "accuracy",
            "scores": scores,
            "selected_variables": selected_variables,
        },
    )

    X_train_selected_full = split_data.X_train_full[selected_variables]
    X_test_selected = split_data.X_test[selected_variables]

    random_forest_model = RandomForestClassifier(
        **best_params,
        random_state=RANDOM_SEED,
        n_jobs=1,
    )
    random_forest_model.fit(X_train_selected_full, split_data.y_train_full)
    joblib.dump(random_forest_model, RF_DIR / "random_forest_model.joblib")

    y_pred_test = random_forest_model.predict(X_test_selected)
    test_accuracy = accuracy_score(split_data.y_test, y_pred_test)

    return {
        "model": "random_forest",
        "validation_accuracy": best_score,
        "best_params": json.dumps(best_params),
        "selected_variables": len(selected_variables),
        "test_accuracy": test_accuracy,
    }


def run_xgboost_pipeline(selected_variables: list[str]) -> dict:
    X_train_selected = split_data.X_train[selected_variables]
    X_validation_selected = split_data.X_validation[selected_variables]

    best_score = -1.0
    best_params = None
    scores = {}

    for n_estimators in XGB_PARAM_GRID["n_estimators"]:
        for learning_rate in XGB_PARAM_GRID["learning_rate"]:
            for max_depth in XGB_PARAM_GRID["max_depth"]:
                for min_child_weight in XGB_PARAM_GRID["min_child_weight"]:
                    for subsample in XGB_PARAM_GRID["subsample"]:
                        for colsample_bytree in XGB_PARAM_GRID["colsample_bytree"]:
                            params = {
                                "n_estimators": n_estimators,
                                "learning_rate": learning_rate,
                                "max_depth": max_depth,
                                "min_child_weight": min_child_weight,
                                "subsample": subsample,
                                "colsample_bytree": colsample_bytree,
                            }
                            xgboost_model = XGBClassifier(
                                **params,
                                objective="binary:logistic",
                                eval_metric="logloss",
                                random_state=RANDOM_SEED,
                                n_jobs=1,
                            )
                            xgboost_model.fit(X_train_selected, split_data.y_train)

                            y_pred_validation = xgboost_model.predict(X_validation_selected)
                            score = accuracy_score(split_data.y_validation, y_pred_validation)
                            scores[str(params)] = score

                            if score > best_score:
                                best_score = score
                                best_params = params

    save_json(
        XGB_DIR / "best_params.json",
        {
            "best_params": best_params,
            "best_score": best_score,
            "metric": "accuracy",
            "scores": scores,
            "selected_variables": selected_variables,
        },
    )

    X_train_selected_full = split_data.X_train_full[selected_variables]
    X_test_selected = split_data.X_test[selected_variables]

    xgboost_model = XGBClassifier(
        **best_params,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=RANDOM_SEED,
        n_jobs=1,
    )
    xgboost_model.fit(X_train_selected_full, split_data.y_train_full)
    joblib.dump(xgboost_model, XGB_DIR / "xgboost_model.joblib")

    y_pred_test = xgboost_model.predict(X_test_selected)
    test_accuracy = accuracy_score(split_data.y_test, y_pred_test)

    return {
        "model": "xgboost",
        "validation_accuracy": best_score,
        "best_params": json.dumps(best_params),
        "selected_variables": len(selected_variables),
        "test_accuracy": test_accuracy,
    }


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


def run_neural_network_pipeline(selected_variables: list[str]) -> dict:
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)
    torch.manual_seed(RANDOM_SEED)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(RANDOM_SEED)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    X_train_selected = split_data.X_train[selected_variables]
    X_validation_selected = split_data.X_validation[selected_variables]
    X_train_full_selected = split_data.X_train_full[selected_variables]
    X_test_selected = split_data.X_test[selected_variables]

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train_selected)
    X_validation_scaled = scaler.transform(X_validation_selected)

    X_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32)
    y_train_tensor = torch.tensor(np.asarray(split_data.y_train), dtype=torch.long)
    X_validation_tensor = torch.tensor(X_validation_scaled, dtype=torch.float32)
    y_validation_tensor = torch.tensor(np.asarray(split_data.y_validation), dtype=torch.long)

    input_dim = X_train_tensor.shape[1]
    train_dataset = TensorDataset(X_train_tensor, y_train_tensor)

    def train_and_validate(params):
        model = NeuralNet(
            input_dim=input_dim,
            hidden_dim_1=params["hidden_dim_1"],
            hidden_dim_2=params["hidden_dim_2"],
            dropout=params["dropout"],
        ).to(device)

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
        patience_counter = 0

        for _ in range(NN_EPOCHS):
            model.train()
            for batch_X, batch_y in train_loader:
                batch_X = batch_X.to(device)
                batch_y = batch_y.to(device)

                optimizer.zero_grad()
                outputs = model(batch_X)
                loss = criterion(outputs, batch_y)
                loss.backward()
                optimizer.step()

            model.eval()
            with torch.no_grad():
                val_outputs = model(X_validation_tensor.to(device))
                val_loss = criterion(val_outputs, y_validation_tensor.to(device)).item()

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_model_state = copy.deepcopy(model.state_dict())
                patience_counter = 0
            else:
                patience_counter += 1

            if patience_counter >= NN_PATIENCE:
                break

        model.load_state_dict(best_model_state)
        model.eval()
        with torch.no_grad():
            logits = model(X_validation_tensor.to(device))
            y_pred = torch.argmax(logits, dim=1).cpu().numpy()

        validation_accuracy = accuracy_score(split_data.y_validation, y_pred)
        return model, best_model_state, best_val_loss, validation_accuracy

    best_score = -1.0
    best_params = None
    best_model_state = None
    scores = {}

    for hidden_dim_1 in NN_PARAM_GRID["hidden_dim_1"]:
        for hidden_dim_2 in NN_PARAM_GRID["hidden_dim_2"]:
            for dropout in NN_PARAM_GRID["dropout"]:
                for learning_rate in NN_PARAM_GRID["learning_rate"]:
                    for weight_decay in NN_PARAM_GRID["weight_decay"]:
                        for batch_size in NN_PARAM_GRID["batch_size"]:
                            params = {
                                "hidden_dim_1": hidden_dim_1,
                                "hidden_dim_2": hidden_dim_2,
                                "dropout": dropout,
                                "learning_rate": learning_rate,
                                "weight_decay": weight_decay,
                                "batch_size": batch_size,
                            }
                            _, state_dict, validation_loss, validation_accuracy = train_and_validate(params)
                            scores[str(params)] = {
                                "validation_accuracy": validation_accuracy,
                                "validation_loss": validation_loss,
                            }

                            if validation_accuracy > best_score:
                                best_score = validation_accuracy
                                best_params = params
                                best_model_state = state_dict

    save_json(
        NN_DIR / "best_params.json",
        {
            "best_params": best_params,
            "best_score": best_score,
            "metric": "accuracy",
            "scores": scores,
            "selected_variables": selected_variables,
            "input_dim": int(input_dim),
        },
    )
    torch.save(best_model_state, NN_DIR / "best_model_state.pt")

    final_scaler = StandardScaler()
    X_train_full_scaled = final_scaler.fit_transform(X_train_full_selected)
    X_test_scaled = final_scaler.transform(X_test_selected)

    X_train_full_tensor = torch.tensor(X_train_full_scaled, dtype=torch.float32)
    y_train_full_tensor = torch.tensor(np.asarray(split_data.y_train_full), dtype=torch.long)
    X_test_tensor = torch.tensor(X_test_scaled, dtype=torch.float32)

    final_model = NeuralNet(
        input_dim=X_train_full_tensor.shape[1],
        hidden_dim_1=best_params["hidden_dim_1"],
        hidden_dim_2=best_params["hidden_dim_2"],
        dropout=best_params["dropout"],
    ).to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(
        final_model.parameters(),
        lr=best_params["learning_rate"],
        weight_decay=best_params["weight_decay"],
    )
    final_loader = DataLoader(
        TensorDataset(X_train_full_tensor, y_train_full_tensor),
        batch_size=best_params["batch_size"],
        shuffle=True,
    )

    for _ in range(NN_EPOCHS):
        final_model.train()
        for batch_X, batch_y in final_loader:
            batch_X = batch_X.to(device)
            batch_y = batch_y.to(device)
            optimizer.zero_grad()
            outputs = final_model(batch_X)
            loss = criterion(outputs, batch_y)
            loss.backward()
            optimizer.step()

    final_model.eval()
    with torch.no_grad():
        logits = final_model(X_test_tensor.to(device))
        y_pred_test = torch.argmax(logits, dim=1).cpu().numpy()

    test_accuracy = accuracy_score(split_data.y_test, y_pred_test)

    return {
        "model": "neural_network",
        "validation_accuracy": best_score,
        "best_params": json.dumps(best_params),
        "selected_variables": len(selected_variables),
        "test_accuracy": test_accuracy,
    }


def print_split_summary() -> None:
    print("Split summary:")
    print("  train:", split_data.y_train.value_counts().sort_index().to_dict())
    print("  validation:", split_data.y_validation.value_counts().sort_index().to_dict())
    print("  test:", split_data.y_test.value_counts().sort_index().to_dict())
    print("  train_full:", split_data.y_train_full.value_counts().sort_index().to_dict())
    print()


def print_summary(results: list[dict]) -> None:
    summary_df = pd.DataFrame(results)
    if "validation_accuracy" not in summary_df.columns:
        summary_df["validation_accuracy"] = np.nan
    if "selected_variables" not in summary_df.columns:
        summary_df["selected_variables"] = np.nan
    summary_df = summary_df.sort_values("test_accuracy", ascending=False).reset_index(drop=True)

    print("\nSummary:")
    print(summary_df.to_string(index=False))


def main():
    ensure_directories()
    print_split_summary()

    results = []

    print("Running null model...")
    results.append(run_null_model())

    print("Running lasso validation, training and performance...")
    lasso_result, selected_variables = run_lasso_pipeline()
    results.append(lasso_result)

    print("Running logistic regression training and performance...")
    results.append(run_logistic_pipeline(selected_variables))

    print("Running random forest validation, training and performance...")
    results.append(run_random_forest_pipeline(selected_variables))

    print("Running XGBoost validation, training and performance...")
    results.append(run_xgboost_pipeline(selected_variables))

    if RUN_NEURAL_NETWORK:
        print("Running neural network validation, training and performance...")
        results.append(run_neural_network_pipeline(selected_variables))

    print_summary(results)


if __name__ == "__main__":
    main()
