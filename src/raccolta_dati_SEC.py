#%%
import pandas as pd
import requests
import time
import json
from datetime import datetime
from functools import reduce
import config as cfg
# -----------------------------
# CONFIG
# -----------------------------
# Final CSV path for the SEC panel dataset.
OUTPUT_PATH = cfg.SEC_DATASET

# Metrics exported to the final dataset.
# These are the "core" variables kept after filtering out low-coverage ones.
OUTPUT_METRICS = [
    "Revenue",
    "NetIncome",
    "OperatingIncome",
    "Assets",
    "Equity",
    "Cash",
    "OperatingCashFlow",
    "EPS",
    "SharesOutstanding",
    "TotalDebt",
]

START_DATE = "2021-01-01"   # ~5 anni
END_DATE = datetime.now().strftime("%Y-%m-%d")

# SEC APIs require a meaningful User-Agent.
HEADERS = {
    "User-Agent": "andrea.leonardi632@edu.unito.it"
}

# Accepted filing types when scanning SEC companyfacts.
ALLOWED_FORMS = {"10-K", "10-Q", "10-K/A", "10-Q/A", "20-F", "20-F/A", "6-K", "6-K/A"}

# Flow metrics are reported over a time interval and need duration-based filtering.
FLOW_METRICS = {"Revenue", "NetIncome", "OperatingIncome", "OperatingCashFlow"}

# Standard fiscal period labels used by many SEC facts.
FISCAL_PERIODS = {"Q1", "Q2", "Q3", "FY"}

# Candidate XBRL tags for each final metric.
# The order matters because earlier tags are preferred when multiple tags match.
METRICS = {
    "Revenue": [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "Revenue",
        "Revenues",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
        "SalesRevenueServicesNet",
    ],
    "NetIncome": [
        "NetIncomeLoss",
        "ProfitLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
    ],

    "OperatingIncome": [
        "OperatingIncomeLoss",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    ],

    "Assets": [
        "Assets",
        "AssetsTotal",
    ],
    "Equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "StockholdersEquityAttributableToParent",
    ],
    "TotalDebt": [
        "LongTermObligations",
        "LongTermObligationsNoncurrent",
        "LongTermDebtNoncurrent",
        "LongTermDebtAndCapitalLeaseObligations",
        "LongTermDebtAndCapitalLeaseObligationsNoncurrent",
        "LongTermDebt",
        "LongTermObligationsCurrent",
        "LongTermDebtAndCapitalLeaseObligationsCurrent",
        "DebtCurrent",
        "ShortTermBorrowings",
        "ShortTermDebt",
        "Debt",
    ],
    "Cash": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
        "Cash",
    ],
    "OperatingCashFlow": [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByOperatingActivities",
        "NetCashProvidedByUsedInContinuingOperations",
    ],
    "EPS": [
        "EarningsPerShareDiluted",
        "EarningsPerShareBasic",
    ],
    "SharesOutstanding": [
        "CommonStockSharesOutstanding",
        "EntityCommonStockSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingBasic",
    ],
}

# Extra tags collected only to derive a more complete TotalDebt value.
AUXILIARY_METRICS = {
    "DebtLongTerm": [
        "LongTermObligations",
        "LongTermObligationsNoncurrent",
        "LongTermDebtAndCapitalLeaseObligations",
        "LongTermDebtAndCapitalLeaseObligationsNoncurrent",
        "LongTermDebt",
        "LongTermDebtNoncurrent",
    ],
    "DebtCurrentPart": [
        "LongTermObligationsCurrent",
        "LongTermDebtAndCapitalLeaseObligationsCurrent",
        "DebtCurrent",
        "ShortTermBorrowings",
        "ShortTermDebt",
        "LongTermDebtMaturitiesRepaymentsOfPrincipalInNextTwelveMonths",
    ],
}

# Combined lookup used by the generic extraction helpers.
METRIC_TAGS = {**METRICS, **AUXILIARY_METRICS}

# Ticker/CIK mapping cache validity window.
CACHE_MAX_AGE_DAYS = 7

# -----------------------------
# LOAD TICKERS
# -----------------------------




# Load the local company universe.
companies = pd.read_csv(cfg.ENT)

# Temporary filter used to run the pipeline only on a subset of companies.
companies = companies[companies["Ticker"].isin(["APD", "AZN", "BABA", "BHP", "CEG", "CRH", "DLR", "GEV", "GS", "MNST", "O", "PLTR", "RY", "SHEL", "TTE", "V", "XEL", "WFC", "MS"])]

# Normalize tickers and keep unique values before querying SEC.
tickers = (
    companies["Ticker"]
    .dropna()
    .astype(str)
    .str.strip()
    .str.upper()
    .drop_duplicates()
    .tolist()
)

# Static metadata reused later in the coverage report.
ticker_metadata = companies[["Ticker", "Company_name", "Sector"]].drop_duplicates()

# -----------------------------
# LOAD TICKER → CIK MAP
# -----------------------------

print("Downloading SEC ticker mapping...")

ticker_map_url = "https://www.sec.gov/files/company_tickers.json"

# Reuse a single HTTP session to reduce connection overhead across many requests.
session = requests.Session()
session.headers.update(HEADERS)


def load_ticker_map() -> dict:
    """Load the SEC ticker-to-CIK map, using a local JSON cache when possible."""
    cache_path = cfg.SEC_TICKER_MAP_CACHE
    if cache_path.exists():
        cache_age = datetime.now().timestamp() - cache_path.stat().st_mtime
        if cache_age <= CACHE_MAX_AGE_DAYS * 24 * 60 * 60:
            with cache_path.open("r", encoding="utf-8") as f:
                return json.load(f)

    ticker_map_resp = session.get(ticker_map_url, timeout=30)
    ticker_map_resp.raise_for_status()
    ticker_map = ticker_map_resp.json()
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(ticker_map, f)
    return ticker_map


ticker_map = load_ticker_map()

ticker_to_cik = {}
for entry in ticker_map.values():
    ticker = str(entry["ticker"]).upper()
    cik = str(entry["cik_str"]).zfill(10)
    # Save multiple ticker variants because some providers use dots,
    # hyphens, or no separators for the same security class.
    for variant in {
        ticker,
        ticker.replace(".", "-"),
        ticker.replace("-", "."),
        ticker.replace(".", ""),
        ticker.replace("-", ""),
    }:
        ticker_to_cik[variant] = cik

# -----------------------------
# FUNCTIONS
# -----------------------------

def normalize_ticker(ticker: str) -> list[str]:
    """Generate common ticker variants used when matching local tickers to SEC mapping."""
    base = str(ticker).strip().upper()
    return list(
        dict.fromkeys(
            [
                base,
                base.replace(".", "-"),
                base.replace("-", "."),
                base.replace(".", ""),
                base.replace("-", ""),
            ]
        )
    )


def select_best_unit(units_dict: dict, metric_name: str) -> tuple[str | None, list | None]:
    """Pick the most suitable unit for a metric, preferring the expected standard one."""
    if not units_dict:
        return None, None

    if metric_name == "EPS":
        preferred_units = ["USD/shares"]
    elif metric_name == "SharesOutstanding":
        preferred_units = ["shares"]
    else:
        preferred_units = ["USD"]

    for unit in preferred_units:
        if unit in units_dict and units_dict[unit]:
            return unit, units_dict[unit]

    unit_priority = sorted(
        units_dict.items(),
        key=lambda item: (
            0 if len(item[0]) <= 6 else 1,
            -len(item[1]),
            item[0],
        ),
    )

    for unit_name, entries in unit_priority:
        if entries:
            return unit_name, entries

    return None, None


def select_preferred_period_rows(series: pd.DataFrame, metric_name: str) -> pd.DataFrame:
    """
    Keep the most meaningful rows for a metric.

    For flow metrics, this function tries to prioritize quarterly facts, while still
    allowing semiannual or yearly fallback when quarterly data is not available.
    """
    if series.empty:
        return series

    if "fp" not in series.columns:
        return series

    if metric_name not in FLOW_METRICS:
        return series

    series = series.copy()
    series["period_kind"] = pd.NA
    series.loc[series["duration_days"].between(70, 110, inclusive="both"), "period_kind"] = "Q"
    series.loc[series["duration_days"].between(150, 220, inclusive="both"), "period_kind"] = "H"
    series.loc[series["duration_days"].between(300, 380, inclusive="both"), "period_kind"] = "Y"

    standard_periods = series[series["fp"].isin(FISCAL_PERIODS)].copy()
    if not standard_periods.empty:
        preferred_rows = []

        for (_, fp), group in standard_periods.groupby(["fy", "fp"], dropna=False):
            group = group.sort_values(["filed", "form"])

            if fp == "FY":
                preferred = group[group["period_kind"] == "Y"]
            else:
                preferred = group[group["period_kind"] == "Q"]

            chosen = preferred if not preferred.empty else group
            preferred_rows.append(chosen.tail(1))

        if preferred_rows:
            return pd.concat(preferred_rows, ignore_index=True)

    fallback_periods = series[series["period_kind"].isin(["Q", "H", "Y"])].copy()
    if fallback_periods.empty:
        return series.iloc[0:0]

    fallback_periods = fallback_periods.sort_values(["Date", "filed", "form"])
    return fallback_periods.drop_duplicates(subset=["Date"], keep="last")


def collect_metric_rows(metric_name: str, facts_group: dict) -> pd.DataFrame:
    """
    Collect all candidate facts for one metric inside one accounting taxonomy.

    This step does not decide the final series yet; it just gathers valid rows,
    normalizes dates, removes segmented facts, and keeps tag metadata.
    """
    metric_rows = []

    for tag_priority, tag in enumerate(METRIC_TAGS[metric_name]):
        if tag not in facts_group:
            continue

        unit_name, entries = select_best_unit(facts_group[tag].get("units", {}), metric_name)
        if not entries:
            continue

        for entry in entries:
            end_date = entry.get("end")
            form = entry.get("form")
            val = entry.get("val")

            # Ignore incomplete records.
            if end_date is None or val is None:
                continue

            # Ignore filing types outside the forms handled by this pipeline.
            if form not in ALLOWED_FORMS:
                continue

            # Ignore segmented facts and keep only consolidated company-level values.
            if entry.get("segment") is not None:
                continue

            metric_rows.append(
                {
                    "start": entry.get("start"),
                    "Date": end_date,
                    metric_name: val,
                    "form": form,
                    "fy": entry.get("fy"),
                    "fp": entry.get("fp"),
                    "frame": entry.get("frame"),
                    "filed": entry.get("filed"),
                    "tag": tag,
                    "unit": unit_name,
                    "tag_priority": tag_priority,
                }
            )

    if not metric_rows:
        return pd.DataFrame()

    rows_df = pd.DataFrame(metric_rows)
    rows_df["start"] = pd.to_datetime(rows_df["start"], errors="coerce")
    rows_df["Date"] = pd.to_datetime(rows_df["Date"], errors="coerce")
    rows_df["filed"] = pd.to_datetime(rows_df["filed"], errors="coerce")
    rows_df = rows_df.sort_values(["Date", "filed", "tag_priority", "form"])

    # Duration is used only for flow metrics to classify facts as quarterly,
    # semiannual or yearly.
    rows_df["duration_days"] = (rows_df["Date"] - rows_df["start"]).dt.days
    rows_df = select_preferred_period_rows(rows_df, metric_name)
    if rows_df.empty:
        return rows_df

    rows_df[metric_name] = pd.to_numeric(rows_df[metric_name], errors="coerce")
    rows_df = rows_df.dropna(subset=[metric_name])
    rows_df["Date"] = pd.to_datetime(rows_df["Date"]).dt.normalize()
    return rows_df


def build_metric_series(
    metric_name: str,
    facts_group: dict,
) -> tuple[pd.DataFrame | None, str | None, str | None]:
    """
    Build a single clean time series for one metric within one taxonomy.

    If multiple facts land on the same date, the function keeps the most recent
    and highest-priority candidate.
    """
    rows_df = collect_metric_rows(metric_name, facts_group)
    if rows_df.empty:
        return None, None, None

    rows_df = rows_df.sort_values(["Date", "filed", "tag_priority", "form"])
    rows_df = rows_df.drop_duplicates(subset=["Date", "fy", "fp", "frame"], keep="last")
    rows_df = rows_df.drop_duplicates(subset=["Date"], keep="last")

    best_tag = rows_df["tag"].mode().iloc[0] if not rows_df["tag"].mode().empty else rows_df.iloc[-1]["tag"]
    best_unit = rows_df.iloc[-1]["unit"]
    series = rows_df[["Date", metric_name]]
    return series, best_tag, best_unit


def apply_derived_metrics(fin_df: pd.DataFrame) -> pd.DataFrame:
    """Fill TotalDebt using debt components when the direct tag is missing."""
    fin_df = fin_df.copy()
    debt_components = [col for col in ("DebtLongTerm", "DebtCurrentPart") if col in fin_df.columns]
    if "TotalDebt" in fin_df.columns and debt_components:
        has_any_debt_component = fin_df[debt_components].notna().any(axis=1)
        debt_sum = fin_df[debt_components].fillna(0).sum(axis=1)
        total_debt_missing = fin_df["TotalDebt"].isna() & has_any_debt_component
        fin_df.loc[total_debt_missing, "TotalDebt"] = debt_sum[total_debt_missing]

    return fin_df


def build_coverage_report(dataset: pd.DataFrame) -> pd.DataFrame:
    """Create a per-ticker, per-metric coverage summary for diagnostics."""
    rows = []

    for ticker, group in dataset.groupby("Ticker"):
        total_rows = len(group)
        for metric_name in OUTPUT_METRICS:
            available = int(group[metric_name].notna().sum()) if metric_name in group.columns else 0
            rows.append(
                {
                    "Ticker": ticker,
                    "Rows": total_rows,
                    "Metric": metric_name,
                    "Available": available,
                    "Missing": total_rows - available,
                    "CoveragePct": round((available * 100) / total_rows, 2) if total_rows else 0,
                }
            )

    report = pd.DataFrame(rows)
    report = report.merge(ticker_metadata, on="Ticker", how="left")
    return report.sort_values(["CoveragePct", "Ticker", "Metric"], ascending=[True, True, True])


def get_sec_financials(cik: str) -> tuple[pd.DataFrame | None, list[str]]:
    """
    Download SEC companyfacts for one CIK and assemble the metrics table.

    The function searches standard taxonomies first and then scans any remaining
    taxonomy present in the payload to support foreign issuers and custom facts.
    """
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    debug_notes = []

    try:
        # The shared session reduces latency across many sequential API calls.
        r = session.get(url, timeout=30)
        if r.status_code != 200:
            return None, [f"companyfacts status {r.status_code}"]
        data = r.json()
    except Exception as exc:
        return None, [f"request error: {exc}"]

    all_series = []
    facts = data.get("facts", {})
    fact_groups = []
    preferred_fact_groups = ("us-gaap", "ifrs-full")

    # Standard accounting taxonomies are preferred when available.
    for group_name in preferred_fact_groups:
        group = facts.get(group_name, {})
        if group:
            fact_groups.append((group_name, group))

    # Any remaining taxonomy is kept as fallback for edge cases such as foreign issuers.
    for group_name, group in sorted(facts.items()):
        if group_name in preferred_fact_groups:
            continue
        if group:
            fact_groups.append((group_name, group))

    if not fact_groups:
        return None, ["missing facts in companyfacts payload"]

    metric_order = OUTPUT_METRICS + list(AUXILIARY_METRICS)

    # Build one series per metric, stopping at the first taxonomy that yields usable data.
    for metric_name in metric_order:
        selected_series = None
        used_group = None
        used_tag = None
        used_unit = None

        for group_name, group in fact_groups:
            series, tag, unit_name = build_metric_series(metric_name, group)
            if series is not None:
                selected_series = series
                used_group = group_name
                used_tag = tag
                used_unit = unit_name
                break

        if selected_series is None:
            debug_notes.append(f"{metric_name}: not found")
            continue

        all_series.append(selected_series)
        debug_notes.append(
            f"{metric_name}: {used_group}/{used_tag} [{used_unit}] rows={len(selected_series)}"
        )

    if not all_series:
        return None, debug_notes

    # Merge all metric series into a single per-company time series.
    fin_df = reduce(
        lambda left, right: pd.merge(left, right, on="Date", how="outer"),
        all_series,
    ).sort_values("Date")

    # Restrict the dataset to the analysis window.
    fin_df = fin_df[(fin_df["Date"] >= START_DATE) & (fin_df["Date"] <= END_DATE)]

    if fin_df.empty:
        return None, debug_notes + ["all rows filtered by date range"]

    fin_df = fin_df.drop_duplicates(subset=["Date"], keep="last")

    # Derive missing debt values and drop auxiliary columns before returning.
    fin_df = apply_derived_metrics(fin_df)
    output_columns = [column for column in OUTPUT_METRICS if column in fin_df.columns]
    fin_df = fin_df[["Date", *output_columns]]
    fin_df = fin_df.set_index("Date")

    return fin_df, debug_notes


def build_company_dataset(ticker: str, cik: str) -> pd.DataFrame | None:
    """Attach the ticker label to the company-level financial time series."""
    fin_df, debug_notes = get_sec_financials(cik)
    if fin_df is None or fin_df.empty:
        if debug_notes:
            print(f"  -> Details: {'; '.join(debug_notes)}")
        return None
    fin_df = fin_df.copy()
    fin_df["Ticker"] = ticker
    fin_df = fin_df.reset_index()

    return fin_df

# -----------------------------
# BUILD DATASET
# -----------------------------

# Collect successful datasets and keep a log of failed tickers.
all_data = []
failed_tickers = []

for i, ticker in enumerate(tickers, start=1):
    ticker = str(ticker).strip().upper()
    print(f"[{i}/{len(tickers)}] Processing {ticker}")

    cik = None
    # Try all normalized ticker variants until a matching CIK is found.
    for ticker_variant in normalize_ticker(ticker):
        cik = ticker_to_cik.get(ticker_variant)
        if cik is not None:
            break

    if cik is None:
        print(f"  -> CIK not found for {ticker}")
        failed_tickers.append((ticker, "CIK not found after ticker normalization"))
        continue

    try:
        company_df = build_company_dataset(ticker, cik)

        if company_df is None or company_df.empty:
            print(f"  -> No usable data for {ticker}")
            failed_tickers.append((ticker, "No usable data"))
            continue

        all_data.append(company_df)

    except Exception as e:
        print(f"  -> Error for {ticker}: {e}")
        failed_tickers.append((ticker, str(e)))

    # Small delay to stay polite with SEC infrastructure.
    time.sleep(0.2)

# -----------------------------
# SAVE OUTPUT
# -----------------------------

if all_data:
    # Concatenate all companies into the final panel dataset.
    dataset = pd.concat(all_data, ignore_index=True)
    dataset = dataset.sort_values(["Ticker", "Date"])
    dataset.to_csv(OUTPUT_PATH, index=False)

    # Save a separate coverage report to inspect data quality by ticker and metric.
    coverage_report = build_coverage_report(dataset)
    coverage_report.to_csv(cfg.SEC_COVERAGE_REPORT, index=False)
    print(f"\nDataset saved to: {OUTPUT_PATH}")
    print(f"Total rows: {len(dataset)}")
    print(f"Coverage report saved to: {cfg.SEC_COVERAGE_REPORT}")
else:
    print("\nNo dataset created.")

if failed_tickers:
    # Persist failed tickers for easier debugging and reruns.
    failed_df = pd.DataFrame(failed_tickers, columns=["Ticker", "Reason"])
    failed_df.to_csv(cfg.SEC_FAILED_TICKERS, index=False)
    print("Failed tickers report saved to: sec_dataset_failed_tickers.csv")


# %%
