import pandas as pd
import requests
import time
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------

TOP200_PATH = "data/possible_enterprises/enterprises.csv"
OUTPUT_PATH = "sec_dataset.csv"

START_DATE = "2021-01-01"   # ~5 anni
END_DATE = datetime.now().strftime("%Y-%m-%d")

HEADERS = {
    "User-Agent": "financial-research-project your_email@example.com"
}

# -----------------------------
# LOAD TICKERS
# -----------------------------

companies = pd.read_csv(TOP200_PATH)
tickers = companies["Ticker"].dropna().astype(str).tolist()

# -----------------------------
# LOAD TICKER → CIK MAP
# -----------------------------

print("Downloading SEC ticker mapping...")

ticker_map_url = "https://www.sec.gov/files/company_tickers.json"
ticker_map_resp = requests.get(ticker_map_url, headers=HEADERS, timeout=30)
ticker_map_resp.raise_for_status()
ticker_map = ticker_map_resp.json()

ticker_to_cik = {}
for entry in ticker_map.values():
    ticker = str(entry["ticker"]).upper()
    cik = str(entry["cik_str"]).zfill(10)
    ticker_to_cik[ticker] = cik

# -----------------------------
# FUNCTIONS
# -----------------------------

def get_sec_financials(cik: str) -> pd.DataFrame | None:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception:
        return None

    us_gaap = data.get("facts", {}).get("us-gaap", {})
    if not us_gaap:
        return None

    metrics = {
        "Revenue": [
            "Revenues",
            "SalesRevenueNet",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "SalesRevenueGoodsNet",
            "SalesRevenueServicesNet"
        ],
        "NetIncome": [
            "NetIncomeLoss",
            "ProfitLoss",
            "NetIncomeLossAvailableToCommonStockholdersBasic"
        ],
        "OperatingIncome": [
            "OperatingIncomeLoss",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"
        ],
        "GrossProfit": [
            "GrossProfit",
            "GrossProfitLoss"
        ],
        "Assets": [
            "Assets",
            "AssetsTotal"
        ],
        "Equity": [
            "StockholdersEquity",
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
            "StockholdersEquityAttributableToParent"
        ],
        "CurrentAssets": [
            "AssetsCurrent",
            "CurrentAssets"
        ],
        "CurrentLiabilities": [
            "LiabilitiesCurrent",
            "CurrentLiabilities"
        ],
        "Cash": [
            "CashAndCashEquivalentsAtCarryingValue",
            "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
            "Cash"
        ],
        "OperatingCashFlow": [
            "NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByOperatingActivities",
            "NetCashProvidedByUsedInContinuingOperations"
        ],
        "EPS": [
            "EarningsPerShareDiluted",
            "EarningsPerShareBasic"
        ],
        "SharesOutstanding": [
            "CommonStockSharesOutstanding",
            "EntityCommonStockSharesOutstanding",
            "WeightedAverageNumberOfSharesOutstandingBasic"
        ],
    }

    all_series = []

    for col_name, possible_tags in metrics.items():
        selected_tag = None
        for tag in possible_tags:
            if tag in us_gaap:
                selected_tag = tag
                break

        if selected_tag is None:
            continue

        units_dict = us_gaap[selected_tag].get("units", {})

        candidate_units = ["USD", "USD/shares", "shares"]
        entries = None
        for unit in candidate_units:
            if unit in units_dict:
                entries = units_dict[unit]
                break

        if not entries:
            continue

        rows = []
        for e in entries:
            end_date = e.get("end")
            form = e.get("form")
            val = e.get("val")

            if end_date is None or val is None:
                continue

            if form not in {"10-K", "10-Q", "10-K/A", "10-Q/A"}:
                continue

            rows.append({
                "Date": pd.to_datetime(end_date),
                col_name: val,
                "form": form
            })

        if not rows:
            continue

        s = pd.DataFrame(rows).sort_values("Date")
        s = s.drop_duplicates(subset=["Date"], keep="last")
        s = s[["Date", col_name]].set_index("Date")
        all_series.append(s)

    if not all_series:
        return None

    fin_df = pd.concat(all_series, axis=1).sort_index()
    fin_df = fin_df[(fin_df.index >= START_DATE) & (fin_df.index <= END_DATE)]

    if fin_df.empty:
        return None

    return fin_df


def build_company_dataset(ticker: str, cik: str) -> pd.DataFrame | None:
    fin_df = get_sec_financials(cik)
    if fin_df is None or fin_df.empty:
        return None

    fin_df = fin_df.copy()
    fin_df["Ticker"] = ticker
    fin_df = fin_df.reset_index()

    return fin_df

# -----------------------------
# BUILD DATASET
# -----------------------------

all_data = []
failed_tickers = []

for i, ticker in enumerate(tickers, start=1):
    ticker = ticker.upper()
    print(f"[{i}/{len(tickers)}] Processing {ticker}...")

    cik = ticker_to_cik.get(ticker)
    if cik is None:
        print(f"  -> CIK not found for {ticker}")
        failed_tickers.append((ticker, "CIK not found"))
        continue

    try:
        company_df = build_company_dataset(ticker, cik)

        if company_df is None or company_df.empty:
            print(f"  -> No usable data for {ticker}")
            failed_tickers.append((ticker, "No usable data"))
            continue

        all_data.append(company_df)
        print(f"  -> OK, {len(company_df)} rows")

    except Exception as e:
        print(f"  -> Error for {ticker}: {e}")
        failed_tickers.append((ticker, str(e)))

    time.sleep(0.2)

# -----------------------------
# SAVE OUTPUT
# -----------------------------

if all_data:
    dataset = pd.concat(all_data, ignore_index=True)
    dataset = dataset.sort_values(["Ticker", "Date"])
    dataset.to_csv(OUTPUT_PATH, index=False)
    print(f"\nDataset saved to: {OUTPUT_PATH}")
    print(f"Total rows: {len(dataset)}")
else:
    print("\nNo dataset created.")

if failed_tickers:
    failed_df = pd.DataFrame(failed_tickers, columns=["Ticker", "Reason"])
    failed_df.to_csv("sec_dataset_failed_tickers.csv", index=False)
    print("Failed tickers report saved to: sec_dataset_failed_tickers.csv")