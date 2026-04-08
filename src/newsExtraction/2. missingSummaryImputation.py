import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg


def main():
    # Carico il dataset news prodotto dallo step di raccolta.
    df = pd.read_csv(cfg.NEWS_ARTICLES)

    # Considero missing sia i NaN veri sia eventuali stringhe vuote o fatte solo di spazi.
    summary_missing_mask = df["Summary"].isna() | df["Summary"].astype(str).str.strip().eq("")

    # Quando il summary manca, copio direttamente il titolo dell'articolo.
    df.loc[summary_missing_mask, "Summary"] = df.loc[summary_missing_mask, "Headline"]

    # Mantengo un ordinamento coerente con il resto della pipeline.
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
