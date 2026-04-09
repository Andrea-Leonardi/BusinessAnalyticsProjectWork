#%%
"""
Pipeline runner per la cartella dataExtraction.

Flusso del codice:
1. salva una fotografia dello stato attuale dei file e dei dataset principali;
2. esegue 1.FMP_companySelection.py per aggiornare enterprises.csv;
3. elimina solo i file aziendali che appartengono a ticker usciti dal nuovo universo;
4. lancia gli altri step della pipeline;
5. confronta lo stato pre-run e post-run e stampa un riepilogo finale di cio che e cambiato.

L'obiettivo del report finale non e solo dire che la pipeline e andata a buon fine,
ma spiegare in modo leggibile:
- quali ticker sono entrati o usciti da enterprises.csv;
- quali file aziendali sono stati cancellati o creati;
- come sono cambiati i dataset aggregati;
- quali ticker hanno piu o meno righe nel full dataset rispetto a prima della run.
"""

import subprocess
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config as cfg


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SINGLE_COMPANY_OUTPUT_RULES = [
    {
        "label": "price_files",
        "title": "File prezzi aziendali",
        "directory": cfg.SINGLE_COMPANY_PRICES,
        "suffix": "Prices.csv",
    },
    {
        "label": "financial_files",
        "title": "File financials aziendali",
        "directory": cfg.SINGLE_COMPANY_FINANCIALS,
        "suffix": "Financials.csv",
    },
    {
        "label": "full_data_files",
        "title": "File full data aziendali",
        "directory": cfg.SINGLE_COMPANY_FULL_DATA,
        "suffix": "data.csv",
    },
]

DATASET_SNAPSHOT_RULES = [
    {
        "label": "enterprises",
        "title": "enterprises.csv",
        "path": cfg.ENT,
        "ticker_columns": ["Ticker"],
    },
    {
        "label": "all_price_data",
        "title": "allPriceData.csv",
        "path": cfg.ALL_PRICE_DATA,
        "ticker_columns": ["Ticker"],
    },
    {
        "label": "raw_financials",
        "title": "financialsDataRaw.csv",
        "path": cfg.FMP_RAW_FINANCIALS,
        "ticker_columns": ["requested_symbol", "symbol", "Ticker"],
    },
    {
        "label": "processed_financials",
        "title": "financialsData.csv",
        "path": cfg.FMP_FINANCIALS,
        "ticker_columns": ["symbol", "Ticker"],
    },
    {
        "label": "full_data",
        "title": "fulldata.csv",
        "path": cfg.FULL_DATA,
        "ticker_columns": ["Ticker", "symbol"],
    },
    {
        "label": "full_data_ml",
        "title": "fulldata_ml.csv",
        "path": cfg.FULL_DATA_ML,
        "ticker_columns": [],
    },
]

COMPANY_SELECTION_SCRIPT = cfg.DATA_EXTRACTION_SRC / "1.FMP_companySelection.py"
REMAINING_PIPELINE_SCRIPTS = [
    cfg.DATA_EXTRACTION_SRC / "2.priceDataGathering.py",
    cfg.DATA_EXTRACTION_SRC / "3.FMP_financialsDataGathering.py",
    cfg.DATA_EXTRACTION_SRC / "4.FMP_financialsDataProcessing.py",
    cfg.DATA_EXTRACTION_SRC / "6.FMP_dataMerge.py",
]

TOP_CHANGED_TICKERS_TO_PRINT = 15


# ---------------------------------------------------------------------------
# Generic Helpers
# ---------------------------------------------------------------------------

def load_tickers_from_enterprises() -> set[str]:
    # Leggo il perimetro aziende corrente da enterprises.csv.
    if not cfg.ENT.exists():
        return set()

    enterprises_df = pd.read_csv(cfg.ENT, usecols=["Ticker"])
    tickers = enterprises_df["Ticker"].dropna().astype(str).str.strip().str.upper()
    tickers = tickers[tickers.ne("")]
    return set(tickers.drop_duplicates().tolist())


def extract_ticker_from_filename(filename: str, suffix: str) -> str:
    # Ricavo il ticker dal naming standard dei file aziendali.
    if not filename.endswith(suffix):
        return ""

    return filename[: -len(suffix)].strip().upper()


def format_ticker_list(tickers: list[str], max_items: int = 12) -> str:
    # Formatto le liste ticker in modo leggibile senza stampare righe infinite.
    if not tickers:
        return "nessuno"

    ordered_tickers = sorted(dict.fromkeys(tickers))
    if len(ordered_tickers) <= max_items:
        return ", ".join(ordered_tickers)

    visible_items = ", ".join(ordered_tickers[:max_items])
    hidden_count = len(ordered_tickers) - max_items
    return f"{visible_items}, ... (+{hidden_count})"


def format_signed_int(value: int) -> str:
    # Rendo piu leggibile il delta tra prima e dopo la run.
    return f"{value:+d}"


def format_ticker_row_changes(
    row_changes: list[dict[str, int]],
    max_items: int = TOP_CHANGED_TICKERS_TO_PRINT,
) -> str:
    # Formatto i ticker che hanno avuto variazioni di righe nei dataset.
    if not row_changes:
        return "nessuno"

    formatted_rows = []
    for change in row_changes[:max_items]:
        formatted_rows.append(
            f"{change['ticker']} ({change['before']} -> {change['after']}, {format_signed_int(change['delta'])})"
        )

    if len(row_changes) > max_items:
        formatted_rows.append(f"... (+{len(row_changes) - max_items})")

    return ", ".join(formatted_rows)


def find_first_existing_column(
    dataframe: pd.DataFrame,
    candidates: list[str],
) -> str | None:
    # Trovo la prima colonna ticker disponibile nel dataset.
    for column in candidates:
        if column in dataframe.columns:
            return column
    return None


def load_csv_row_count(file_path: Path) -> int | None:
    # Leggo il numero di righe di un CSV. Se il file e corrotto ritorno None.
    try:
        return len(pd.read_csv(file_path))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Snapshot Helpers
# ---------------------------------------------------------------------------

def snapshot_company_files() -> dict[str, dict[str, dict[str, int | str | None]]]:
    # Salvo per ogni cartella aziendale i ticker presenti e il numero di righe
    # del relativo file, cosi posso confrontare la situazione prima e dopo la run.
    snapshots: dict[str, dict[str, dict[str, int | str | None]]] = {}

    for output_rule in SINGLE_COMPANY_OUTPUT_RULES:
        label = output_rule["label"]
        output_dir = output_rule["directory"]
        suffix = output_rule["suffix"]
        output_dir.mkdir(parents=True, exist_ok=True)

        rule_snapshot: dict[str, dict[str, int | str | None]] = {}
        for csv_file in output_dir.glob("*.csv"):
            ticker = extract_ticker_from_filename(csv_file.name, suffix)
            if not ticker:
                continue

            rule_snapshot[ticker] = {
                "path": str(csv_file),
                "rows": load_csv_row_count(csv_file),
            }

        snapshots[label] = rule_snapshot

    return snapshots


def snapshot_dataset(path: Path, ticker_columns: list[str]) -> dict[str, object]:
    # Salvo le metriche principali di un dataset aggregato.
    if not path.exists():
        return {
            "exists": False,
            "path": str(path),
            "rows": 0,
            "columns": 0,
            "ticker_count": 0,
            "ticker_column": None,
            "ticker_counts": {},
            "read_error": None,
        }

    try:
        dataset_df = pd.read_csv(path)
    except Exception as exc:
        return {
            "exists": True,
            "path": str(path),
            "rows": 0,
            "columns": 0,
            "ticker_count": 0,
            "ticker_column": None,
            "ticker_counts": {},
            "read_error": str(exc),
        }

    ticker_column = find_first_existing_column(dataset_df, ticker_columns)
    ticker_counts: dict[str, int] = {}
    if ticker_column is not None:
        cleaned_tickers = (
            dataset_df[ticker_column].dropna().astype(str).str.strip().str.upper()
        )
        cleaned_tickers = cleaned_tickers[cleaned_tickers.ne("")]
        ticker_counts = cleaned_tickers.value_counts().sort_index().to_dict()

    return {
        "exists": True,
        "path": str(path),
        "rows": len(dataset_df),
        "columns": len(dataset_df.columns),
        "ticker_count": len(ticker_counts),
        "ticker_column": ticker_column,
        "ticker_counts": ticker_counts,
        "read_error": None,
    }


def snapshot_all_datasets() -> dict[str, dict[str, object]]:
    # Salvo lo stato di tutti i dataset aggregati usati nel report finale.
    dataset_snapshots: dict[str, dict[str, object]] = {}

    for dataset_rule in DATASET_SNAPSHOT_RULES:
        dataset_snapshots[dataset_rule["label"]] = snapshot_dataset(
            path=dataset_rule["path"],
            ticker_columns=dataset_rule["ticker_columns"],
        )

    return dataset_snapshots


# ---------------------------------------------------------------------------
# Comparison Helpers
# ---------------------------------------------------------------------------

def compare_company_rule_snapshot(
    before_snapshot: dict[str, dict[str, int | str | None]],
    after_snapshot: dict[str, dict[str, int | str | None]],
) -> dict[str, object]:
    # Confronto i file aziendali di una singola cartella prima e dopo la run.
    before_tickers = set(before_snapshot)
    after_tickers = set(after_snapshot)

    created_tickers = sorted(after_tickers - before_tickers)
    removed_tickers = sorted(before_tickers - after_tickers)

    changed_rows = []
    for ticker in sorted(before_tickers & after_tickers):
        before_rows = before_snapshot[ticker]["rows"]
        after_rows = after_snapshot[ticker]["rows"]
        if before_rows == after_rows:
            continue

        before_value = before_rows if isinstance(before_rows, int) else -1
        after_value = after_rows if isinstance(after_rows, int) else -1
        changed_rows.append(
            {
                "ticker": ticker,
                "before": before_rows,
                "after": after_rows,
                "delta": after_value - before_value,
            }
        )

    changed_rows.sort(
        key=lambda row: (
            -abs(row["delta"]),
            row["ticker"],
        )
    )

    return {
        "created_tickers": created_tickers,
        "removed_tickers": removed_tickers,
        "changed_rows": changed_rows,
        "before_count": len(before_tickers),
        "after_count": len(after_tickers),
    }


def compare_dataset_snapshot(
    before_snapshot: dict[str, object],
    after_snapshot: dict[str, object],
) -> dict[str, object]:
    # Confronto i dataset aggregati prima e dopo la run.
    before_rows = int(before_snapshot["rows"])
    after_rows = int(after_snapshot["rows"])
    before_columns = int(before_snapshot["columns"])
    after_columns = int(after_snapshot["columns"])
    before_ticker_count = int(before_snapshot["ticker_count"])
    after_ticker_count = int(after_snapshot["ticker_count"])

    before_ticker_counts = before_snapshot["ticker_counts"]
    after_ticker_counts = after_snapshot["ticker_counts"]

    before_tickers = set(before_ticker_counts)
    after_tickers = set(after_ticker_counts)

    ticker_row_changes = []
    for ticker in sorted(before_tickers | after_tickers):
        before_value = int(before_ticker_counts.get(ticker, 0))
        after_value = int(after_ticker_counts.get(ticker, 0))
        delta = after_value - before_value
        if delta == 0:
            continue

        ticker_row_changes.append(
            {
                "ticker": ticker,
                "before": before_value,
                "after": after_value,
                "delta": delta,
            }
        )

    positive_row_changes = sorted(
        [row for row in ticker_row_changes if row["delta"] > 0],
        key=lambda row: (-row["delta"], row["ticker"]),
    )
    negative_row_changes = sorted(
        [row for row in ticker_row_changes if row["delta"] < 0],
        key=lambda row: (row["delta"], row["ticker"]),
    )

    return {
        "before_rows": before_rows,
        "after_rows": after_rows,
        "row_delta": after_rows - before_rows,
        "before_columns": before_columns,
        "after_columns": after_columns,
        "column_delta": after_columns - before_columns,
        "before_ticker_count": before_ticker_count,
        "after_ticker_count": after_ticker_count,
        "ticker_count_delta": after_ticker_count - before_ticker_count,
        "created_tickers": sorted(after_tickers - before_tickers),
        "removed_tickers": sorted(before_tickers - after_tickers),
        "positive_row_changes": positive_row_changes,
        "negative_row_changes": negative_row_changes,
        "before_read_error": before_snapshot["read_error"],
        "after_read_error": after_snapshot["read_error"],
    }


# ---------------------------------------------------------------------------
# Cleanup Helper
# ---------------------------------------------------------------------------

def delete_invalid_single_company_files(valid_tickers: set[str]) -> dict[str, list[str]]:
    # Elimino solo i file aziendali il cui ticker non fa piu parte del perimetro
    # corrente di enterprises.csv e tengo traccia dettagliata di cosa sparisce.
    deleted_files_by_rule: dict[str, list[str]] = {}

    for output_rule in SINGLE_COMPANY_OUTPUT_RULES:
        label = output_rule["label"]
        output_dir = output_rule["directory"]
        suffix = output_rule["suffix"]
        output_dir.mkdir(parents=True, exist_ok=True)

        deleted_tickers: list[str] = []
        for csv_file in output_dir.glob("*.csv"):
            ticker = extract_ticker_from_filename(csv_file.name, suffix)
            if not ticker:
                continue

            if ticker not in valid_tickers:
                csv_file.unlink()
                deleted_tickers.append(ticker)

        deleted_files_by_rule[label] = sorted(dict.fromkeys(deleted_tickers))

    return deleted_files_by_rule


# ---------------------------------------------------------------------------
# Printing Helpers
# ---------------------------------------------------------------------------

def print_dataset_change_report(
    dataset_title: str,
    dataset_change: dict[str, object],
) -> None:
    # Stampo una sezione del report finale dedicata a un dataset aggregato.
    print(f"\n{dataset_title}")
    print(
        f"- Righe: {dataset_change['before_rows']} -> {dataset_change['after_rows']} "
        f"({format_signed_int(int(dataset_change['row_delta']))})"
    )
    print(
        f"- Colonne: {dataset_change['before_columns']} -> {dataset_change['after_columns']} "
        f"({format_signed_int(int(dataset_change['column_delta']))})"
    )
    print(
        f"- Ticker: {dataset_change['before_ticker_count']} -> {dataset_change['after_ticker_count']} "
        f"({format_signed_int(int(dataset_change['ticker_count_delta']))})"
    )

    if dataset_change["created_tickers"]:
        print(f"- Ticker aggiunti: {format_ticker_list(dataset_change['created_tickers'])}")
    if dataset_change["removed_tickers"]:
        print(f"- Ticker rimossi: {format_ticker_list(dataset_change['removed_tickers'])}")

    if dataset_change["positive_row_changes"]:
        print(
            f"- Ticker con piu righe: "
            f"{format_ticker_row_changes(dataset_change['positive_row_changes'])}"
        )
    if dataset_change["negative_row_changes"]:
        print(
            f"- Ticker con meno righe: "
            f"{format_ticker_row_changes(dataset_change['negative_row_changes'])}"
        )

    if (
        not dataset_change["created_tickers"]
        and not dataset_change["removed_tickers"]
        and not dataset_change["positive_row_changes"]
        and not dataset_change["negative_row_changes"]
        and dataset_change["column_delta"] == 0
        and dataset_change["row_delta"] == 0
    ):
        print("- Nessuna differenza rispetto a prima della run.")

    if dataset_change["before_read_error"] or dataset_change["after_read_error"]:
        print(
            "- Nota lettura dataset:"
            f" before={dataset_change['before_read_error']},"
            f" after={dataset_change['after_read_error']}"
        )


def print_final_report(
    previous_enterprise_tickers: set[str],
    current_enterprise_tickers: set[str],
    deleted_files_by_rule: dict[str, list[str]],
    company_file_changes: dict[str, dict[str, object]],
    dataset_changes: dict[str, dict[str, object]],
) -> None:
    # Riepilogo finale della run, costruito dal confronto pre/post.
    added_enterprise_tickers = sorted(current_enterprise_tickers - previous_enterprise_tickers)
    removed_enterprise_tickers = sorted(previous_enterprise_tickers - current_enterprise_tickers)

    print("\n" + "=" * 90)
    print("RIEPILOGO FINALE DATA EXTRACTION")
    print("=" * 90)

    print("\nUniverso aziende")
    print(f"- Ticker prima della run: {len(previous_enterprise_tickers)}")
    print(f"- Ticker dopo la run: {len(current_enterprise_tickers)}")
    print(f"- Ticker aggiunti da company selection: {format_ticker_list(added_enterprise_tickers)}")
    print(f"- Ticker rimossi da company selection: {format_ticker_list(removed_enterprise_tickers)}")

    print("\nPulizia file fuori universo")
    for output_rule in SINGLE_COMPANY_OUTPUT_RULES:
        label = output_rule["label"]
        title = output_rule["title"]
        deleted_tickers = deleted_files_by_rule.get(label, [])
        print(f"- {title}: cancellati {len(deleted_tickers)} file -> {format_ticker_list(deleted_tickers)}")

    print("\nFile aziendali aggiornati")
    for output_rule in SINGLE_COMPANY_OUTPUT_RULES:
        label = output_rule["label"]
        title = output_rule["title"]
        change = company_file_changes[label]

        print(f"- {title}: {change['before_count']} -> {change['after_count']} file")
        print(f"  Creati: {len(change['created_tickers'])} -> {format_ticker_list(change['created_tickers'])}")
        print(f"  Rimossi: {len(change['removed_tickers'])} -> {format_ticker_list(change['removed_tickers'])}")
        print(
            f"  File con righe cambiate: {len(change['changed_rows'])}"
            f" -> {format_ticker_row_changes(change['changed_rows'])}"
        )

    print("\nDataset aggregati")
    for dataset_rule in DATASET_SNAPSHOT_RULES:
        label = dataset_rule["label"]
        title = dataset_rule["title"]
        print_dataset_change_report(title, dataset_changes[label])

    print("\nLettura pratica della run")
    full_data_change = dataset_changes["full_data"]
    full_data_ml_change = dataset_changes["full_data_ml"]
    price_change = company_file_changes["price_files"]
    financial_change = company_file_changes["financial_files"]

    print(
        f"- Prezzi scaricati o ricreati per {len(price_change['created_tickers'])} ticker: "
        f"{format_ticker_list(price_change['created_tickers'])}"
    )
    print(
        f"- Financials aziendali creati o ricreati per {len(financial_change['created_tickers'])} ticker: "
        f"{format_ticker_list(financial_change['created_tickers'])}"
    )
    print(
        f"- Nel dataset finale fulldata.csv ci sono "
        f"{format_signed_int(int(full_data_change['row_delta']))} righe rispetto a prima."
    )
    print(
        f"- Nel dataset finale fulldata_ml.csv ci sono "
        f"{format_signed_int(int(full_data_ml_change['row_delta']))} righe rispetto a prima."
    )


# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------

# Salvo lo stato iniziale dell'universo, dei file aziendali e dei dataset.
previous_enterprise_tickers = load_tickers_from_enterprises()
company_file_snapshot_before = snapshot_company_files()
dataset_snapshot_before = snapshot_all_datasets()


# ---------------------------------------------------------------------------
# Run Company Selection First
# ---------------------------------------------------------------------------

print(f"\nRunning {COMPANY_SELECTION_SCRIPT.name}...")
subprocess.run([sys.executable, str(COMPANY_SELECTION_SCRIPT)], check=True)


# ---------------------------------------------------------------------------
# Clean Invalid Single-Company CSV Files
# ---------------------------------------------------------------------------

current_enterprise_tickers = load_tickers_from_enterprises()
deleted_files_by_rule = delete_invalid_single_company_files(current_enterprise_tickers)

deleted_files_total = sum(len(tickers) for tickers in deleted_files_by_rule.values())
print(f"Deleted {deleted_files_total} invalid single-company CSV files.")


# ---------------------------------------------------------------------------
# Run Remaining Pipeline
# ---------------------------------------------------------------------------

for script_path in REMAINING_PIPELINE_SCRIPTS:
    print(f"\nRunning {script_path.name}...")
    subprocess.run([sys.executable, str(script_path)], check=True)


# ---------------------------------------------------------------------------
# Build Final Run Report
# ---------------------------------------------------------------------------

company_file_snapshot_after = snapshot_company_files()
dataset_snapshot_after = snapshot_all_datasets()

company_file_changes = {}
for output_rule in SINGLE_COMPANY_OUTPUT_RULES:
    label = output_rule["label"]
    company_file_changes[label] = compare_company_rule_snapshot(
        before_snapshot=company_file_snapshot_before[label],
        after_snapshot=company_file_snapshot_after[label],
    )

dataset_changes = {}
for dataset_rule in DATASET_SNAPSHOT_RULES:
    label = dataset_rule["label"]
    dataset_changes[label] = compare_dataset_snapshot(
        before_snapshot=dataset_snapshot_before[label],
        after_snapshot=dataset_snapshot_after[label],
    )

print("\nPipeline completed successfully.")
print_final_report(
    previous_enterprise_tickers=previous_enterprise_tickers,
    current_enterprise_tickers=current_enterprise_tickers,
    deleted_files_by_rule=deleted_files_by_rule,
    company_file_changes=company_file_changes,
    dataset_changes=dataset_changes,
)


# %%
