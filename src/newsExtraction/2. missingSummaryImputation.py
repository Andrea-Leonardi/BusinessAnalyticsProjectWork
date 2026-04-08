import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg


def main():
    # Carico il dataset news gia creato.
    df = pd.read_csv(cfg.NEWS_ARTICLES)

    # Considero missing sia i NaN sia le stringhe vuote o composte solo da spazi.
    summary_missing_mask = df["Summary"].isna() | df["Summary"].astype(str).str.strip().eq("")

    # Quando manca il summary, uso direttamente il titolo della news.
    df.loc[summary_missing_mask, "Summary"] = df.loc[summary_missing_mask, "Headline"].fillna("")

    # Ordino e salvo di nuovo il file aggiornato.
    df.sort_values(by=["Ticker", "Date"], ascending=[True, True], inplace=True)
    df.to_csv(cfg.NEWS_ARTICLES, index=False, encoding="utf-8-sig")

    print(
        "Imputazione summary completata:",
        {
            "rows": len(df),
            "filled_from_headline": int(summary_missing_mask.sum()),
            "output": str(cfg.NEWS_ARTICLES),
        },
    )


if __name__ == "__main__":
    main()
