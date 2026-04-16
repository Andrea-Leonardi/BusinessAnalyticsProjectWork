from pathlib import Path
import json
import sys

import numpy as np
from sklearn.metrics import accuracy_score

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import y_test


current_dir = Path(__file__).resolve().parent
performance_path = current_dir / "performance.json"
PREDICTED_CLASS = 1


def evaluate_and_save_performance():
    y_pred_test = np.full(len(y_test), PREDICTED_CLASS, dtype=int)
    test_accuracy = float(accuracy_score(y_test, y_pred_test))

    performance = {
        "model": "always_one",
        "metric": "accuracy",
        "predicted_class": PREDICTED_CLASS,
        "test_accuracy": test_accuracy,
    }

    with open(performance_path, "w", encoding="utf-8") as f:
        json.dump(performance, f, indent=4)

    print("Always-one predicted class:", PREDICTED_CLASS)
    print("Test accuracy:", test_accuracy)
    return performance


if __name__ == "__main__":
    evaluate_and_save_performance()
