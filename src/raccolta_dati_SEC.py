#%%
import pandas as pd
import requests
import time
import json
import re
from html import unescape
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
    "Assets",
    "Equity",
    "Cash",
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
ALLOWED_FORMS = {
    "10-K",
    "10-Q",
    "10-K/A",
    "10-Q/A",
    "20-F",
    "20-F/A",
    "40-F",
    "40-F/A",
    "6-K",
    "6-K/A",
}

# Flow metrics are reported over a time interval and need duration-based filtering.
FLOW_METRICS = {"Revenue", "NetIncome"}

# Standard fiscal period labels used by many SEC facts.
FISCAL_PERIODS = {"Q1", "Q2", "Q3", "FY"}

# Candidate XBRL tags for each final metric.
# The order matters because earlier tags are preferred when multiple tags match.
METRICS = {
    "Revenue": [
        "RegulatedAndUnregulatedOperatingRevenue",
        "OperatingRevenues",
        "ElectricDomesticRegulatedRevenue",
        "GasDomesticRegulatedRevenue",
        "OtherSalesRevenueNet",
        "UnregulatedOperatingRevenue",
        "RevenueFromContractsWithCustomers",
        "RevenuesNetOfInterestExpense",
        "NetRevenues",
        "TotalNetRevenues",
        "NoninterestIncome",
        "TotalNoninterestRevenues",
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


    "Assets": [
        "Assets",
        "AssetsTotal",
    ],
    "Equity": [
        "Equity",
        "TotalEquity",
        "EquityAttributableToOwnersOfParent",
        "EquityAttributableToOwnersOfTheParent",
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "StockholdersEquityAttributableToParent",
    ],
    "TotalDebt": [
        "LoansAndBorrowings",
        "Borrowings",
        "LongtermBorrowings",
        "CurrentBorrowingsAndCurrentPortionOfNoncurrentBorrowings",
        "CurrentPortionOfLongtermBorrowings",
        "DebtInstrumentCarryingAmount",
        "DebtInstrumentFaceAmount",
        "FinanceLeaseLiability",
        "FinanceLeaseLiabilityNoncurrent",
        "FinanceLeaseObligation",
        "FinanceLeaseObligationNoncurrent",
        "LongTermDebtAndFinanceLeaseObligations",
        "LongTermDebtAndFinanceLeaseObligationsNoncurrent",
        "NotesPayable",
        "NotesPayableNoncurrent",
        "DebtSecurities",
        "DebtSecuritiesNoncurrent",
        "MortgageLoansOnRealEstate",
        "MortgageLoansOnRealEstateNoncurrent",
        "RevolvingCreditFacility",
        "RevolvingCreditFacilityNoncurrent",
        "LineOfCreditFacility",
        "LineOfCreditFacilityNoncurrent",
        "TermLoan",
        "TermLoanNoncurrent",
        "TermLoans",
        "TermLoansNoncurrent",
        "SeniorNotes",
        "SeniorNotesNoncurrent",
        "UnsecuredDebt",
        "OtherDebt",
        "OtherDebtNoncurrent",
        "LongTermLoansFromBank",
        "ConvertibleDebtNoncurrent",
        "LongTermObligations",
        "LongTermObligationsNoncurrent",
        "LoansAndBorrowingsNoncurrent",
        "BorrowingsNoncurrent",
        "LongTermDebtNoncurrent",
        "LongTermDebtAndCapitalLeaseObligations",
        "LongTermDebtAndCapitalLeaseObligationsNoncurrent",
        "LongTermDebt",
        "Debt",
    ],
    "Cash": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
        "Cash",
    ],
    "EPS": [
        "BasicEarningsLossPerShareFromContinuingOperations",
        "DilutedEarningsLossPerShareFromContinuingOperations",
        "IncomeLossFromContinuingOperationsPerBasicShare",
        "IncomeLossFromContinuingOperationsPerDilutedShare",
        "BasicEarningsPerShare",
        "DilutedEarningsPerShare",
        "BasicAndDilutedEarningsPerShare",
        "BasicEarningsLossPerShare",
        "DilutedEarningsLossPerShare",
        "EarningsPerShareDiluted",
        "EarningsPerShareBasic",
    ],
    "SharesOutstanding": [
        "OrdinarySharesNumber",
        "SharesOutstanding",
        "WeightedAverageShares",
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingDiluted",
        "WeightedAverageNumberOfBasicSharesOutstanding",
        "WeightedAverageNumberOfShareOutstandingBasicAndDiluted",
        "IssuedCapitalNumberOfShares",
        "NumberOfSharesOutstanding",
        "WeightedAverageNumberOfOrdinarySharesOutstandingBasic",
        "WeightedAverageNumberOfOrdinarySharesOutstandingDiluted",
        "CommonStockSharesOutstanding",
        "EntityCommonStockSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingBasic",
    ],
}

# Extra tags collected only to derive a more complete TotalDebt value.
AUXILIARY_METRICS = {
    "DebtLongTerm": [
        "LoansAndBorrowings",
        "Borrowings",
        "LongtermBorrowings",
        "DebtInstrumentCarryingAmount",
        "DebtInstrumentFaceAmount",
        "FinanceLeaseLiability",
        "FinanceLeaseLiabilityNoncurrent",
        "FinanceLeaseObligation",
        "FinanceLeaseObligationNoncurrent",
        "LongTermDebtAndFinanceLeaseObligations",
        "LongTermDebtAndFinanceLeaseObligationsNoncurrent",
        "NotesPayable",
        "NotesPayableNoncurrent",
        "DebtSecurities",
        "DebtSecuritiesNoncurrent",
        "MortgageLoansOnRealEstate",
        "MortgageLoansOnRealEstateNoncurrent",
        "RevolvingCreditFacility",
        "RevolvingCreditFacilityNoncurrent",
        "LineOfCreditFacility",
        "LineOfCreditFacilityNoncurrent",
        "TermLoan",
        "TermLoanNoncurrent",
        "TermLoans",
        "TermLoansNoncurrent",
        "SeniorNotes",
        "SeniorNotesNoncurrent",
        "UnsecuredDebt",
        "OtherDebt",
        "OtherDebtNoncurrent",
        "LeaseLiabilities",
        "LongTermLoansFromBank",
        "ConvertibleDebtNoncurrent",
        "LongTermObligations",
        "LongTermObligationsNoncurrent",
        "LoansAndBorrowingsNoncurrent",
        "BorrowingsNoncurrent",
        "LeaseLiabilitiesNoncurrent",
        "LongTermDebtAndCapitalLeaseObligations",
        "LongTermDebtAndCapitalLeaseObligationsNoncurrent",
        "LongTermDebt",
        "LongTermDebtNoncurrent",
    ],
    "DebtCurrentPart": [
        "LoansAndBorrowingsCurrent",
        "BorrowingsCurrent",
        "CurrentBorrowingsAndCurrentPortionOfNoncurrentBorrowings",
        "CurrentPortionOfLongtermBorrowings",
        "FinanceLeaseLiabilityCurrent",
        "FinanceLeaseObligationCurrent",
        "LongTermDebtAndFinanceLeaseObligationsCurrent",
        "NotesPayableCurrent",
        "DebtSecuritiesCurrent",
        "MortgageLoansOnRealEstateCurrent",
        "RevolvingCreditFacilityCurrent",
        "LineOfCreditFacilityCurrent",
        "TermLoanCurrent",
        "TermLoansCurrent",
        "SeniorNotesCurrent",
        "OtherDebtCurrent",
        "LeaseLiabilitiesCurrent",
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

# Metrics below full coverage are included in the candidates diagnostics report.
MIN_COMPLETE_COVERAGE = 1.0

# Inline XBRL fallback tags used when companyfacts omits EPS or share-count facts.
INLINE_XBRL_FALLBACK_TAGS = {
    "EPS": [
        "us-gaap:EarningsPerShareDiluted",
        "us-gaap:EarningsPerShareBasic",
        "us-gaap:IncomeLossFromContinuingOperationsPerDilutedShare",
        "us-gaap:IncomeLossFromContinuingOperationsPerBasicShare",
    ],
    "SharesOutstanding": [
        "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
        "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
        "us-gaap:CommonStockSharesOutstanding",
        "dei:EntityCommonStockSharesOutstanding",
    ],
}

# Keywords used to inspect available concepts when a core metric is still missing.
MISSING_METRIC_KEYWORDS = {
    "Revenue": ["revenue", "revenues", "interest", "fee", "commission"],
    "EPS": ["earning", "per", "share"],
    "SharesOutstanding": ["share", "ordinary", "outstanding", "issued"],
    "Equity": ["equity", "netasset", "shareholder"],
    "TotalDebt": ["debt", "borrow", "loan", "obligation", "note", "credit", "facility", "term", "financelease", "leaseliabil"],
}

TOTAL_DEBT_EXCLUDE_PATTERNS = [
    "proceeds",
    "repay",
    "payment",
    "provision",
    "gain",
    "loss",
    "expense",
    "income",
    "receivable",
    "allowance",
    "investment",
]

# -----------------------------
# LOAD TICKERS
# -----------------------------




# Load the local company universe.
companies = pd.read_csv(cfg.ENT)

# Temporary filter used to run the pipeline only on a subset of companies.
#companies = companies[companies["Ticker"].isin(["WFC", "MS"])]

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
    selected_periods = pd.DataFrame()
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
            selected_periods = pd.concat(preferred_rows, ignore_index=True)

    fallback_periods = series[series["period_kind"].isin(["Q", "H", "Y"])].copy()
    if fallback_periods.empty:
        return selected_periods if not selected_periods.empty else series.iloc[0:0]

    fallback_periods = fallback_periods.sort_values(["Date", "filed", "form"])
    fallback_periods = fallback_periods.drop_duplicates(subset=["Date"], keep="last")

    if selected_periods.empty:
        return fallback_periods

    missing_dates = ~fallback_periods["Date"].isin(selected_periods["Date"])
    if missing_dates.any():
        selected_periods = pd.concat(
            [selected_periods, fallback_periods.loc[missing_dates]],
            ignore_index=True,
        )

    return selected_periods.sort_values(["Date", "filed", "form"])


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

    rows_df = rows_df.sort_values(
        ["Date", "filed", "tag_priority", "form"],
        ascending=[True, True, False, True],
    )
    rows_df = rows_df.drop_duplicates(subset=["Date", "fy", "fp", "frame"], keep="last")
    rows_df = rows_df.drop_duplicates(subset=["Date"], keep="last")

    best_tag = rows_df["tag"].mode().iloc[0] if not rows_df["tag"].mode().empty else rows_df.iloc[-1]["tag"]
    best_unit = rows_df.iloc[-1]["unit"]
    series = rows_df[["Date", metric_name]]
    return series, best_tag, best_unit


def parse_inline_xbrl_number(raw_value: str, scale: str | None, sign: str | None) -> float | None:
    """Convert an inline XBRL numeric fact into a float."""
    cleaned = re.sub(r"<[^>]+>", "", raw_value or "")
    cleaned = unescape(cleaned).strip()
    cleaned = cleaned.replace(",", "")

    if cleaned in {"", "-", "—", "–"}:
        return None

    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"

    try:
        value = float(cleaned)
    except ValueError:
        return None

    if sign == "-":
        value *= -1

    if scale:
        try:
            value *= 10 ** int(scale)
        except ValueError:
            pass

    return value


def parse_inline_xbrl_contexts(html: str) -> dict:
    """Extract period metadata for inline XBRL contexts."""
    contexts = {}
    context_pattern = re.compile(
        r"<xbrli:context id=\"([^\"]+)\".*?</xbrli:context>",
        re.DOTALL | re.IGNORECASE,
    )

    for match in context_pattern.finditer(html):
        context_id = match.group(1)
        context_block = match.group(0)
        start_match = re.search(r"<xbrli:startDate>([^<]+)</xbrli:startDate>", context_block)
        end_match = re.search(r"<xbrli:endDate>([^<]+)</xbrli:endDate>", context_block)
        instant_match = re.search(r"<xbrli:instant>([^<]+)</xbrli:instant>", context_block)

        contexts[context_id] = {
            "start": start_match.group(1) if start_match else None,
            "end": end_match.group(1) if end_match else (instant_match.group(1) if instant_match else None),
            "instant": instant_match.group(1) if instant_match else None,
            "context_block": context_block,
        }

    return contexts


def extract_inline_xbrl_metric_series(cik: str, metric_name: str) -> pd.DataFrame | None:
    """Fallback extractor for metrics missing from companyfacts but present in inline XBRL filings."""
    tag_names = INLINE_XBRL_FALLBACK_TAGS.get(metric_name)
    if not tag_names:
        return None

    submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    try:
        submissions_resp = session.get(submissions_url, timeout=30)
        submissions_resp.raise_for_status()
        submissions = submissions_resp.json()
    except Exception:
        return None

    recent = submissions.get("filings", {}).get("recent", {})
    if not recent:
        return None

    recent_forms = recent.get("form", [])
    accession_numbers = recent.get("accessionNumber", [])
    primary_documents = recent.get("primaryDocument", [])
    filing_dates = recent.get("filingDate", [])

    filing_rows = []
    candidate_forms = {"10-Q", "10-Q/A", "10-K", "10-K/A"}

    for form, accession_number, primary_document, filing_date in zip(
        recent_forms,
        accession_numbers,
        primary_documents,
        filing_dates,
    ):
        if form not in candidate_forms:
            continue

        accession_compact = accession_number.replace("-", "")
        filing_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_compact}/{primary_document}"

        try:
            filing_resp = session.get(filing_url, timeout=30)
            filing_resp.raise_for_status()
            filing_html = filing_resp.text
        except Exception:
            continue

        contexts = parse_inline_xbrl_contexts(filing_html)
        if not contexts:
            continue

        for tag_name in tag_names:
            fact_pattern = re.compile(
                rf"<ix:nonFraction\b([^>]*)name=\"{re.escape(tag_name)}\"([^>]*)>(.*?)</ix:nonFraction>",
                re.DOTALL | re.IGNORECASE,
            )

            for match in fact_pattern.finditer(filing_html):
                attr_left, attr_right, raw_value = match.groups()
                attrs = f"{attr_left} {attr_right}"
                context_match = re.search(r'contextRef=\"([^\"]+)\"', attrs, re.IGNORECASE)
                scale_match = re.search(r'scale=\"([^\"]+)\"', attrs, re.IGNORECASE)
                sign_match = re.search(r'sign=\"([^\"]+)\"', attrs, re.IGNORECASE)
                if context_match is None:
                    continue

                context_ref = context_match.group(1)
                scale = scale_match.group(1) if scale_match else None
                sign = sign_match.group(1) if sign_match else None
                context_data = contexts.get(context_ref)
                if not context_data or not context_data.get("end"):
                    continue

                value = parse_inline_xbrl_number(raw_value, scale, sign)
                if value is None:
                    continue

                filing_rows.append(
                    {
                        "Date": context_data["end"],
                        metric_name: value,
                        "tag": tag_name,
                        "tag_priority": tag_names.index(tag_name),
                        "context_ref": context_ref,
                        "context_block": context_data["context_block"],
                        "filed": filing_date,
                    }
                )

    if not filing_rows:
        return None

    rows_df = pd.DataFrame(filing_rows)
    rows_df["Date"] = pd.to_datetime(rows_df["Date"], errors="coerce").dt.normalize()
    rows_df["filed"] = pd.to_datetime(rows_df["filed"], errors="coerce")
    rows_df = rows_df.dropna(subset=["Date", metric_name])
    rows_df = rows_df[(rows_df["Date"] >= START_DATE) & (rows_df["Date"] <= END_DATE)]
    if rows_df.empty:
        return None

    # Sum class-based common stock facts when multiple classes are disclosed separately.
    if metric_name == "SharesOutstanding":
        rows_df["is_class_member"] = rows_df["context_block"].str.contains("CommonClass", case=False, na=False)
        class_rows = rows_df[rows_df["is_class_member"]].copy()
        non_class_rows = rows_df[~rows_df["is_class_member"]].copy()

        if not class_rows.empty:
            class_rows = (
                class_rows.groupby("Date", as_index=False)
                .agg({metric_name: "sum", "filed": "max"})
                .assign(tag="inline-xbrl-class-sum", tag_priority=-1)
            )
            rows_df = pd.concat(
                [non_class_rows[["Date", metric_name, "tag", "tag_priority", "filed"]], class_rows],
                ignore_index=True,
            )
        else:
            rows_df = rows_df[["Date", metric_name, "tag", "tag_priority", "filed"]]
    else:
        rows_df["class_rank"] = 2
        rows_df.loc[rows_df["context_block"].str.contains("CommonClassAMember", case=False, na=False), "class_rank"] = 0
        rows_df.loc[
            rows_df["context_block"].str.contains("CommonClassBMember|CommonClassCMember", case=False, na=False),
            "class_rank",
        ] = 1
        rows_df = rows_df[["Date", metric_name, "tag", "tag_priority", "filed", "class_rank"]]

    sort_columns = ["Date", "tag_priority", "filed"]
    ascending = [True, True, False]
    if "class_rank" in rows_df.columns:
        sort_columns.insert(1, "class_rank")
        ascending = [True, True, True, False]

    rows_df = rows_df.sort_values(sort_columns, ascending=ascending)
    rows_df = rows_df.drop_duplicates(subset=["Date"], keep="first")
    return rows_df[["Date", metric_name]]


def apply_derived_metrics(fin_df: pd.DataFrame) -> pd.DataFrame:
    """Fill selected metrics using compatible components when the direct tag is missing."""
    fin_df = fin_df.copy()

    for column in ("TotalDebt", "DebtLongTerm", "DebtCurrentPart", "NetIncome", "EPS", "SharesOutstanding"):
        if column in fin_df.columns:
            fin_df[column] = pd.to_numeric(fin_df[column], errors="coerce")

    debt_components = [col for col in ("DebtLongTerm", "DebtCurrentPart") if col in fin_df.columns]
    if "TotalDebt" in fin_df.columns and debt_components:
        has_any_debt_component = fin_df[debt_components].notna().any(axis=1)
        debt_sum = fin_df[debt_components].fillna(0).sum(axis=1)
        total_debt_needs_update = has_any_debt_component & (
            fin_df["TotalDebt"].isna() | (debt_sum > fin_df["TotalDebt"].fillna(float("-inf")))
        )
        fin_df.loc[total_debt_needs_update, "TotalDebt"] = debt_sum[total_debt_needs_update]

    # Some issuers explicitly report a zero debt balance once and then omit the
    # repeated zero values in later filings. For point-in-time debt, carry that
    # zero forward only until another non-missing debt fact appears.
    if "TotalDebt" in fin_df.columns:
        total_debt_series = fin_df["TotalDebt"].copy()
        for index in range(1, len(total_debt_series)):
            if pd.isna(total_debt_series.iloc[index]) and total_debt_series.iloc[index - 1] == 0:
                total_debt_series.iloc[index] = 0
        fin_df["TotalDebt"] = total_debt_series

    if {"NetIncome", "SharesOutstanding"}.issubset(fin_df.columns):
        valid_shares = fin_df["SharesOutstanding"].notna() & fin_df["SharesOutstanding"].ne(0)
        if "EPS" not in fin_df.columns:
            fin_df["EPS"] = pd.Series(index=fin_df.index, dtype="float64")
        missing_eps = fin_df["EPS"].isna() & fin_df["NetIncome"].notna() & valid_shares
        fin_df.loc[missing_eps, "EPS"] = (
            fin_df.loc[missing_eps, "NetIncome"] / fin_df.loc[missing_eps, "SharesOutstanding"]
        )

    if {"NetIncome", "EPS"}.issubset(fin_df.columns):
        valid_eps = fin_df["EPS"].notna() & fin_df["EPS"].ne(0)
        if "SharesOutstanding" not in fin_df.columns:
            fin_df["SharesOutstanding"] = pd.Series(index=fin_df.index, dtype="float64")
        missing_shares = fin_df["SharesOutstanding"].isna() & fin_df["NetIncome"].notna() & valid_eps
        derived_shares = fin_df.loc[missing_shares, "NetIncome"] / fin_df.loc[missing_shares, "EPS"]
        fin_df.loc[missing_shares, "SharesOutstanding"] = derived_shares.abs()

    return fin_df


def drop_anomalous_rows(fin_df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop sparse rows that are likely SEC transition artifacts rather than real observations.

    The rule is intentionally narrow: rows are removed only when SharesOutstanding
    or Equity is exactly zero and the row contains very few populated core metrics.
    """
    fin_df = fin_df.copy()
    numeric_columns = [column for column in OUTPUT_METRICS if column in fin_df.columns]
    if not numeric_columns:
        return fin_df

    populated_metrics = fin_df[numeric_columns].notna().sum(axis=1)
    zero_shares = (
        fin_df["SharesOutstanding"].notna() & fin_df["SharesOutstanding"].eq(0)
        if "SharesOutstanding" in fin_df.columns
        else pd.Series(False, index=fin_df.index)
    )
    zero_equity = (
        pd.to_numeric(fin_df["Equity"], errors="coerce").notna() & pd.to_numeric(fin_df["Equity"], errors="coerce").eq(0)
        if "Equity" in fin_df.columns
        else pd.Series(False, index=fin_df.index)
    )

    anomalous_mask = (zero_shares | zero_equity) & (populated_metrics <= 3)
    return fin_df.loc[~anomalous_mask]


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


def find_metric_candidates(metric_name: str, facts: dict) -> list[dict]:
    """Inspect available concepts and return likely candidates for a missing metric."""
    keywords = MISSING_METRIC_KEYWORDS.get(metric_name, [])
    if not keywords:
        return []

    candidates = []
    for taxonomy, group in facts.items():
        if not group:
            continue

        for concept_name, concept_data in group.items():
            concept_name_lower = concept_name.lower()
            if not any(keyword in concept_name_lower for keyword in keywords):
                continue

            if metric_name == "TotalDebt":
                if any(pattern in concept_name_lower for pattern in TOTAL_DEBT_EXCLUDE_PATTERNS):
                    continue

            units = concept_data.get("units", {})
            entry_count = sum(len(entries) for entries in units.values())
            candidates.append(
                {
                    "Taxonomy": taxonomy,
                    "Concept": concept_name,
                    "UnitCount": len(units),
                    "EntryCount": entry_count,
                }
            )

    candidates.sort(key=lambda item: (-item["EntryCount"], item["Taxonomy"], item["Concept"]))
    return candidates[:20]


def get_sec_financials(cik: str) -> tuple[pd.DataFrame | None, list[str], dict]:
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
            return None, [f"companyfacts status {r.status_code}"], {}
        data = r.json()
    except Exception as exc:
        return None, [f"request error: {exc}"], {}

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
        return None, ["missing facts in companyfacts payload"], facts

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
        return None, debug_notes, facts

    # Merge all metric series into a single per-company time series.
    fin_df = reduce(
        lambda left, right: pd.merge(left, right, on="Date", how="outer"),
        all_series,
    ).sort_values("Date")

    # Restrict the dataset to the analysis window.
    fin_df = fin_df[(fin_df["Date"] >= START_DATE) & (fin_df["Date"] <= END_DATE)]

    if fin_df.empty:
        return None, debug_notes + ["all rows filtered by date range"], facts

    fin_df = fin_df.drop_duplicates(subset=["Date"], keep="last")

    # Fall back to inline XBRL facts for EPS/share counts when companyfacts omits them.
    for metric_name in ("EPS", "SharesOutstanding"):
        metric_missing = metric_name not in fin_df.columns or not fin_df[metric_name].notna().any()
        if not metric_missing:
            continue

        inline_series = extract_inline_xbrl_metric_series(cik, metric_name)
        if inline_series is None or inline_series.empty:
            continue

        existing_metric = metric_name in fin_df.columns
        fin_df = pd.merge(fin_df, inline_series, on="Date", how="outer", suffixes=("", "_inline"))
        inline_column = f"{metric_name}_inline"

        if existing_metric and inline_column in fin_df.columns:
            fin_df[metric_name] = pd.to_numeric(fin_df[metric_name], errors="coerce")
            fin_df[inline_column] = pd.to_numeric(fin_df[inline_column], errors="coerce")
            fin_df[metric_name] = fin_df[metric_name].combine_first(fin_df[inline_column])
            fin_df = fin_df.drop(columns=[inline_column])
        elif not existing_metric and metric_name in fin_df.columns:
            fin_df[metric_name] = pd.to_numeric(fin_df[metric_name], errors="coerce")
        elif inline_column in fin_df.columns:
            fin_df = fin_df.rename(columns={inline_column: metric_name})
            fin_df[metric_name] = pd.to_numeric(fin_df[metric_name], errors="coerce")

        debug_notes.append(f"{metric_name}: inline-xbrl fallback rows={len(inline_series)}")

    fin_df = fin_df[(fin_df["Date"] >= START_DATE) & (fin_df["Date"] <= END_DATE)]
    fin_df = fin_df.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")

    # Derive missing debt values and drop auxiliary columns before returning.
    fin_df = apply_derived_metrics(fin_df)
    fin_df = drop_anomalous_rows(fin_df)
    output_columns = [column for column in OUTPUT_METRICS if column in fin_df.columns]
    fin_df = fin_df[["Date", *output_columns]]
    fin_df = fin_df.set_index("Date")

    return fin_df, debug_notes, facts


def build_company_dataset(ticker: str, cik: str) -> tuple[pd.DataFrame | None, list[dict]]:
    """Attach the ticker label to the company-level financial time series."""
    fin_df, debug_notes, facts = get_sec_financials(cik)
    if fin_df is None or fin_df.empty:
        if debug_notes:
            print(f"  -> Details: {'; '.join(debug_notes)}")
        return None, []

    missing_metric_candidates = []
    for metric_name in OUTPUT_METRICS:
        if metric_name not in fin_df.columns:
            metric_coverage = 0.0
        else:
            metric_coverage = float(fin_df[metric_name].notna().mean())

        if metric_coverage >= MIN_COMPLETE_COVERAGE:
            continue

        for candidate in find_metric_candidates(metric_name, facts):
            missing_metric_candidates.append(
                {
                    "Ticker": ticker,
                    "Metric": metric_name,
                    "CoveragePct": round(metric_coverage * 100, 2),
                    **candidate,
                }
            )

    fin_df = fin_df.copy()
    fin_df["Ticker"] = ticker
    fin_df = fin_df.reset_index()

    return fin_df, missing_metric_candidates

# -----------------------------
# BUILD DATASET
# -----------------------------

# Collect successful datasets and keep a log of failed tickers.
all_data = []
failed_tickers = []
missing_metric_candidates = []

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
        company_df, company_missing_candidates = build_company_dataset(ticker, cik)

        if company_df is None or company_df.empty:
            print(f"  -> No usable data for {ticker}")
            failed_tickers.append((ticker, "No usable data"))
            continue

        all_data.append(company_df)
        missing_metric_candidates.extend(company_missing_candidates)

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
    valid_frames = [frame for frame in all_data if frame is not None and not frame.empty]
    dataset = pd.concat(valid_frames, ignore_index=True)
    dataset = dataset.sort_values(["Ticker", "Date"])
    dataset.to_csv(OUTPUT_PATH, index=False)

    # Save a separate coverage report to inspect data quality by ticker and metric.
    coverage_report = build_coverage_report(dataset)
    coverage_report.to_csv(cfg.SEC_COVERAGE_REPORT, index=False)
    print(f"\nDataset saved to: {OUTPUT_PATH}")
    print(f"Total rows: {len(dataset)}")
    print(f"Coverage report saved to: {cfg.SEC_COVERAGE_REPORT}")

    if missing_metric_candidates:
        candidates_df = pd.DataFrame(missing_metric_candidates).drop_duplicates()
        candidates_df.to_csv(cfg.SEC_MISSING_TAGS_REPORT, index=False)
        print(f"Missing metric candidates report saved to: {cfg.SEC_MISSING_TAGS_REPORT}")
else:
    print("\nNo dataset created.")

if failed_tickers:
    # Persist failed tickers for easier debugging and reruns.
    failed_df = pd.DataFrame(failed_tickers, columns=["Ticker", "Reason"])
    failed_df.to_csv(cfg.SEC_FAILED_TICKERS, index=False)
    print("Failed tickers report saved to: sec_dataset_failed_tickers.csv")


# %%
