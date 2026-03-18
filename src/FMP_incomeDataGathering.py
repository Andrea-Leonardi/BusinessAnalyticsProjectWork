#%%
import argparse
import os
import time
from pathlib import Path

import pandas as pd
import requests


BASE_URL = "https://financialmodelingprep.com/stable/income-statement"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENTERPRISES_PATH = PROJECT_ROOT / "data" / "possible_enterprises" / "enterprises.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "FMP" / "income_statements"

# Parametri di default per una prima prova veloce.
DEFAULT_PERIOD = "quarter"
DEFAULT_LIMIT = 20
DEFAULT_COMPANY_LIMIT = 10
REQUEST_TIMEOUT = 30


def load_companies(limit: int | None = DEFAULT_COMPANY_LIMIT) -> pd.DataFrame:
    """Legge il file delle aziende e restituisce i ticker da interrogare."""
    if not ENTERPRISES_PATH.exists():
        raise FileNotFoundError(f"File aziende non trovato: {ENTERPRISES_PATH}")

    companies = pd.read_csv(ENTERPRISES_PATH)
    if "Ticker" not in companies.columns:
        raise KeyError("La colonna 'Ticker' non esiste nel file enterprises.csv")

    companies = companies.dropna(subset=["Ticker"]).copy()
    companies["Ticker"] = companies["Ticker"].astype(str).str.strip()
    companies = companies[companies["Ticker"] != ""]

    if limit is not None:
        companies = companies.head(limit).copy()

    return companies


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scarica income statement da Financial Modeling Prep."
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="Lista di ticker da scaricare, ad esempio AAPL MSFT NVDA.",
    )
    parser.add_argument(
        "--period",
        default=DEFAULT_PERIOD,
        choices=["annual", "quarter"],
        help="Periodicita dei bilanci da richiedere.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Numero massimo di statement da scaricare per ticker.",
    )
    parser.add_argument(
        "--company-limit",
        type=int,
        default=DEFAULT_COMPANY_LIMIT,
        help="Numero di aziende da leggere da enterprises.csv se non passi --tickers.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Cartella di output per i CSV generati.",
    )
    return parser.parse_args()


def build_companies_from_tickers(tickers: list[str]) -> pd.DataFrame:
    cleaned_tickers = [ticker.strip().upper() for ticker in tickers if ticker and ticker.strip()]
    if not cleaned_tickers:
        raise ValueError("La lista dei ticker e vuota.")

    return pd.DataFrame({"Ticker": cleaned_tickers})


def fetch_income_statement(
    symbol: str,
    api_key: str,
    period: str = DEFAULT_PERIOD,
    limit: int = DEFAULT_LIMIT,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Scarica income statement storici per un singolo ticker tramite FMP."""
    params = {
        "symbol": symbol,
        "period": period,
        "limit": limit,
        "apikey": api_key,
    }

    http = session or requests.Session()
    response = http.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, list):
        raise ValueError(f"Risposta inattesa da FMP per {symbol}: {payload}")

    if not payload:
        return pd.DataFrame()

    income_df = pd.DataFrame(payload)
    income_df.insert(0, "requested_symbol", symbol)

    for date_col in ("date", "fillingDate", "acceptedDate"):
        if date_col in income_df.columns:
            income_df[date_col] = pd.to_datetime(income_df[date_col], errors="coerce")

    if "date" in income_df.columns:
        income_df = income_df.sort_values("date", ascending=False).reset_index(drop=True)

    return income_df


def download_income_statements(
    companies: pd.DataFrame,
    api_key: str,
    period: str = DEFAULT_PERIOD,
    limit: int = DEFAULT_LIMIT,
    output_dir: Path = OUTPUT_DIR,
    pause_seconds: float = 0.25,
) -> pd.DataFrame:
    """
    Scarica gli income statement per tutti i ticker e salva:
    - un CSV per azienda
    - un CSV aggregato con tutti i record
    """
    period_dir = output_dir / period
    period_dir.mkdir(parents=True, exist_ok=True)

    combined_frames: list[pd.DataFrame] = []
    company_names = {}
    if "Company_name" in companies.columns:
        company_names = companies.set_index("Ticker")["Company_name"].to_dict()

    tickers = companies["Ticker"].dropna().astype(str).str.strip().unique()

    with requests.Session() as session:
        for ticker in tickers:
            print(f"Scarico income statement per {ticker}...")
            try:
                income_df = fetch_income_statement(
                    symbol=ticker,
                    api_key=api_key,
                    period=period,
                    limit=limit,
                    session=session,
                )
            except requests.HTTPError as exc:
                print(f"Errore HTTP per {ticker}: {exc}")
                continue
            except requests.RequestException as exc:
                print(f"Errore di rete per {ticker}: {exc}")
                continue
            except ValueError as exc:
                print(f"Errore sui dati ricevuti per {ticker}: {exc}")
                continue

            if income_df.empty:
                print(f"Nessun dato disponibile per {ticker}")
                continue

            company_name = company_names.get(ticker)
            if company_name:
                income_df.insert(1, "company_name", company_name)

            company_file = period_dir / f"{ticker}_income_statement.csv"
            income_df.to_csv(company_file, index=False)
            combined_frames.append(income_df)

            print(f"Salvato: {company_file}")
            time.sleep(pause_seconds)

    if not combined_frames:
        return pd.DataFrame()

    combined_df = pd.concat(combined_frames, ignore_index=True)
    combined_file = period_dir / f"income_statements_{period}_all_companies.csv"
    combined_df.to_csv(combined_file, index=False)
    print(f"Salvato file aggregato: {combined_file}")

    return combined_df


def main() -> None:
    args = parse_args()
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "Variabile ambiente FMP_API_KEY non trovata. "
            "Impostala prima di eseguire lo script."
        )

    if args.tickers:
        companies = build_companies_from_tickers(args.tickers)
    else:
        companies = load_companies(limit=args.company_limit)

    combined_df = download_income_statements(
        companies=companies,
        api_key=api_key,
        period=args.period,
        limit=args.limit,
        output_dir=args.output_dir,
    )

    if combined_df.empty:
        print("Download completato, ma nessun record e stato salvato.")
        return

    print(
        "Download completato: "
        f"{combined_df['requested_symbol'].nunique()} ticker, "
        f"{len(combined_df)} righe totali."
    )


if __name__ == "__main__":
    main()
