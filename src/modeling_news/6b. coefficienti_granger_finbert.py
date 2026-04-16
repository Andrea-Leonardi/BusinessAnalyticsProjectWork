from pathlib import Path
import sys

import pandas as pd
from statsmodels.tsa.vector_ar.var_model import VAR


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg


MAX_LAG = 3
MIN_EXTRA_OBS = 2
OUTPUT_PATH = cfg.NEWS_EXTRACTION / "granger_finbert_coefficients.csv"

FINBERT_COLUMNS = [
    "AdjClosePrice",
    "NEWS_FINBERT_Negative_Mean",
    "NEWS_FINBERT_Neutral_Mean",
    "NEWS_FINBERT_Positive_Mean",
]


def load_dataset() -> pd.DataFrame:
    df = pd.read_csv(cfg.MODELING_DATASET, parse_dates=["WeekEndingFriday"])
    df = df.sort_values(by=["Ticker", "WeekEndingFriday"]).copy()
    return df


def extract_finbert_coefficients(df: pd.DataFrame, max_lag: int = MAX_LAG) -> pd.DataFrame:
    rows: list[dict] = []

    for ticker, df_ticker in df.groupby("Ticker", sort=True):
        model_data = df_ticker[["WeekEndingFriday", *FINBERT_COLUMNS]].dropna().copy()
        model_data = model_data.set_index("WeekEndingFriday")

        if len(model_data) <= max_lag + MIN_EXTRA_OBS:
            continue

        try:
            fitted_model = VAR(model_data).fit(max_lag)
            params = fitted_model.params["AdjClosePrice"]
        except Exception as exc:
            print(f"[SKIP] {ticker}: {exc}")
            continue

        for lag in range(1, max_lag + 1):
            rows.append(
                {
                    "Ticker": ticker,
                    "lag": lag,
                    "alpha": round(float(params["const"]), 7),
                    "beta_negative": round(
                        float(params[f"L{lag}.NEWS_FINBERT_Negative_Mean"]), 7
                    ),
                    "beta_neutral": round(
                        float(params[f"L{lag}.NEWS_FINBERT_Neutral_Mean"]), 7
                    ),
                    "beta_positive": round(
                        float(params[f"L{lag}.NEWS_FINBERT_Positive_Mean"]), 7
                    ),
                    "n_obs_used": int(len(model_data)),
                }
            )

    return pd.DataFrame(rows)


def main() -> None:
    df = load_dataset()
    df_coefficients = extract_finbert_coefficients(df, max_lag=MAX_LAG)

    if df_coefficients.empty:
        print("Nessun coefficiente FinBERT estratto.")
        return

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_coefficients.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print(f"Coefficienti FinBERT salvati in: {OUTPUT_PATH}")
    print(df_coefficients.head(10))


if __name__ == "__main__":
    main()
