from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from split_data import X_test, y_test, get_model_output_dir

from sklearn.metrics import accuracy_score
import joblib

output_dir = get_model_output_dir(Path(__file__).resolve().parent.name)
performance_path = output_dir / "performance.json"


def evaluate_and_save_performance():
    null_model = joblib.load(output_dir / "null_model.joblib")
    y_pred_test = null_model.predict(X_test)
    predicted_class = int(y_pred_test[0]) if len(y_pred_test) > 0 else None
    test_accuracy = float(accuracy_score(y_test, y_pred_test))

    performance = {
        "model": "null_model",
        "metric": "accuracy",
        "predicted_class": predicted_class,
        "test_accuracy": test_accuracy,
    }

    with open(performance_path, "w", encoding="utf-8") as f:
        json.dump(performance, f, indent=4)

    print("Class predicted by the null model:", predicted_class)
    print("Test accuracy:", test_accuracy)
    return performance


if __name__ == "__main__":
    evaluate_and_save_performance()
