import argparse
import sys
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.gridspec import GridSpec

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config as cfg


OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
DEFAULT_TICKER = "AAPL"
DEFAULT_LOOKBACK_WEEKS = 0
DEFAULT_START_DATE = pd.Timestamp("2021-01-01")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a dark-themed dashboard with stock price, weekly return "
            "distribution, and weekly news volume for a selected company."
        )
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=DEFAULT_TICKER,
        help=f"Ticker to visualize (default: {DEFAULT_TICKER}).",
    )
    parser.add_argument(
        "--weeks",
        type=int,
        default=DEFAULT_LOOKBACK_WEEKS,
        help=(
            "Number of most recent weeks to show in the price and news panels "
            f"(default: {DEFAULT_LOOKBACK_WEEKS}, meaning full history from 2021)."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional custom output path for the PNG file.",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Display the figure interactively after saving it.",
    )
    return parser.parse_args()


def get_company_label(ticker: str) -> str:
    if not cfg.COMPANY_SELECTION_UNIVERSE.exists():
        return ticker

    companies_df = pd.read_csv(cfg.COMPANY_SELECTION_UNIVERSE)
    symbol_column = "symbol" if "symbol" in companies_df.columns else None
    name_column = "companyName" if "companyName" in companies_df.columns else None

    if not symbol_column or not name_column:
        return ticker

    company_match = companies_df.loc[
        companies_df[symbol_column].astype(str).str.upper() == ticker.upper(),
        name_column,
    ]
    if company_match.empty:
        return ticker

    return f"{company_match.iloc[0]} ({ticker.upper()})"


def load_price_data(ticker: str) -> pd.DataFrame:
    price_path = cfg.SINGLE_COMPANY_PRICES / f"{ticker.upper()}Prices.csv"
    if not price_path.exists():
        raise FileNotFoundError(f"Price file not found for ticker {ticker}: {price_path}")

    prices_df = pd.read_csv(price_path)
    if prices_df.empty:
        raise ValueError(f"Price file for ticker {ticker} is empty.")

    prices_df["WeekEndingFriday"] = pd.to_datetime(
        prices_df["WeekEndingFriday"], errors="coerce"
    )
    prices_df = prices_df.dropna(subset=["WeekEndingFriday"]).sort_values(
        "WeekEndingFriday"
    )

    if "AdjClosePrice" in prices_df.columns:
        prices_df["DisplayPrice"] = pd.to_numeric(
            prices_df["AdjClosePrice"], errors="coerce"
        )
    elif "ClosePrice" in prices_df.columns:
        prices_df["DisplayPrice"] = pd.to_numeric(
            prices_df["ClosePrice"], errors="coerce"
        )
    else:
        raise ValueError(
            "The selected price file does not contain AdjClosePrice or ClosePrice."
        )

    if "WeeklyReturn_1W" in prices_df.columns:
        prices_df["WeeklyReturn"] = pd.to_numeric(
            prices_df["WeeklyReturn_1W"], errors="coerce"
        )
    else:
        prices_df["WeeklyReturn"] = prices_df["DisplayPrice"].pct_change()

    prices_df["DisplayPrice"] = prices_df["DisplayPrice"].astype(float)
    prices_df["WeeklyReturn"] = prices_df["WeeklyReturn"].astype(float)

    return prices_df.dropna(subset=["DisplayPrice"]).reset_index(drop=True)


def load_news_data(ticker: str) -> pd.DataFrame:
    raw_news_path = cfg.RAW_NEWS_DATA / f"{ticker.upper()}.csv"

    if raw_news_path.exists():
        news_df = pd.read_csv(raw_news_path)
    elif cfg.NEWS_ARTICLES.exists():
        news_df = pd.read_csv(cfg.NEWS_ARTICLES)
        news_df = news_df.loc[
            news_df["Ticker"].astype(str).str.upper() == ticker.upper()
        ].copy()
    else:
        raise FileNotFoundError("No news source was found in the project data folder.")

    if news_df.empty:
        raise ValueError(f"No news rows found for ticker {ticker}.")

    news_df["Date"] = pd.to_datetime(news_df["Date"], errors="coerce", utc=True)
    news_df = news_df.dropna(subset=["Date"]).copy()
    if news_df.empty:
        raise ValueError(f"No valid news dates found for ticker {ticker}.")

    news_dates_naive = news_df["Date"].dt.tz_convert(None)
    news_df["WeekEndingFriday"] = (
        news_dates_naive.dt.to_period("W-FRI").dt.end_time.dt.normalize()
    )
    return news_df


def aggregate_news_by_week(news_df: pd.DataFrame, price_df: pd.DataFrame) -> pd.DataFrame:
    weekly_news_df = (
        news_df.groupby("WeekEndingFriday", as_index=False)
        .size()
        .rename(columns={"size": "NewsCount"})
    )

    aligned_news_df = price_df[["WeekEndingFriday"]].merge(
        weekly_news_df,
        on="WeekEndingFriday",
        how="left",
    )
    aligned_news_df["NewsCount"] = aligned_news_df["NewsCount"].fillna(0).astype(int)
    return aligned_news_df


def limit_to_recent_weeks(
    price_df: pd.DataFrame,
    news_df: pd.DataFrame,
    weeks: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if weeks <= 0:
        min_week = DEFAULT_START_DATE
        trimmed_price_df = price_df.loc[
            price_df["WeekEndingFriday"] >= min_week
        ].copy()
        trimmed_news_df = news_df.loc[
            news_df["WeekEndingFriday"] >= min_week
        ].copy()
        return trimmed_price_df, trimmed_news_df

    trimmed_price_df = price_df.tail(weeks).copy()
    min_week = max(trimmed_price_df["WeekEndingFriday"].min(), DEFAULT_START_DATE)
    trimmed_price_df = trimmed_price_df.loc[
        trimmed_price_df["WeekEndingFriday"] >= min_week
    ].copy()
    trimmed_news_df = news_df.loc[
        news_df["WeekEndingFriday"] >= min_week
    ].copy()
    return trimmed_price_df, trimmed_news_df


def apply_dark_theme() -> dict[str, str]:
    palette = {
        "figure_bg": "#07111a",
        "panel_bg": "#0f1c2b",
        "grid": "#365067",
        "text": "#e8edf2",
        "muted": "#91a7bd",
        "price": "#4dd0e1",
        "ma": "#ffd166",
        "hist": "#90be6d",
        "news": "#ef476f",
        "news_line": "#ff92ad",
        "accent": "#d7e3f0",
    }

    sns.set_theme(style="darkgrid")
    plt.rcParams.update(
        {
            "figure.facecolor": palette["figure_bg"],
            "axes.facecolor": palette["panel_bg"],
            "axes.edgecolor": palette["grid"],
            "axes.labelcolor": palette["text"],
            "axes.titlecolor": palette["text"],
            "xtick.color": palette["muted"],
            "ytick.color": palette["muted"],
            "grid.color": palette["grid"],
            "grid.alpha": 0.25,
            "text.color": palette["text"],
            "savefig.facecolor": palette["figure_bg"],
            "savefig.edgecolor": palette["figure_bg"],
            "font.size": 11,
            "axes.titleweight": "bold",
            "axes.labelsize": 11,
            "axes.titlesize": 15,
        }
    )
    return palette


def format_return(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{value * 100:+.1f}%"


def build_dashboard(
    company_label: str,
    price_df: pd.DataFrame,
    news_df: pd.DataFrame,
    output_path: Path,
) -> None:
    palette = apply_dark_theme()

    recent_price = price_df["DisplayPrice"].iloc[-1]
    starting_price = price_df["DisplayPrice"].iloc[0]
    cumulative_return = (recent_price / starting_price) - 1 if starting_price else 0.0
    mean_weekly_return = price_df["WeeklyReturn"].dropna().mean()
    total_news = int(news_df["NewsCount"].sum())

    chart_price_df = price_df.copy()
    chart_price_df["MA_6W"] = chart_price_df["DisplayPrice"].rolling(6, min_periods=1).mean()

    fig = plt.figure(figsize=(16, 9), dpi=160)
    fig.patch.set_facecolor(palette["figure_bg"])
    grid = GridSpec(2, 2, figure=fig, height_ratios=[1.35, 1], hspace=0.28, wspace=0.18)

    ax_price = fig.add_subplot(grid[0, :])
    ax_returns = fig.add_subplot(grid[1, 0])
    ax_news = fig.add_subplot(grid[1, 1])

    for axis in (ax_price, ax_returns, ax_news):
        axis.set_facecolor(palette["panel_bg"])
        for spine in axis.spines.values():
            spine.set_color(palette["grid"])
        axis.grid(True, axis="y", linestyle="--", linewidth=0.8)

    ax_price.plot(
        chart_price_df["WeekEndingFriday"],
        chart_price_df["DisplayPrice"],
        color=palette["price"],
        linewidth=2.6,
        label="Adjusted close",
    )
    ax_price.fill_between(
        chart_price_df["WeekEndingFriday"],
        chart_price_df["DisplayPrice"],
        color=palette["price"],
        alpha=0.10,
    )
    ax_price.plot(
        chart_price_df["WeekEndingFriday"],
        chart_price_df["MA_6W"],
        color=palette["ma"],
        linewidth=1.6,
        linestyle="--",
        label="6-week moving average",
    )

    ax_price.set_title("Price Trend")
    ax_price.set_ylabel("Price")
    min_price = chart_price_df["DisplayPrice"].min()
    max_price = chart_price_df["DisplayPrice"].max()
    price_padding = max((max_price - min_price) * 0.12, max_price * 0.03)
    ax_price.set_ylim(min_price - price_padding, max_price + price_padding)
    ax_price.xaxis.set_major_locator(mdates.YearLocator())
    ax_price.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax_price.tick_params(axis="x", rotation=0)
    ax_price.legend(
        loc="upper left",
        frameon=False,
        labelcolor=palette["accent"],
    )

    sns.histplot(
        price_df["WeeklyReturn"].dropna(),
        bins=16,
        kde=True,
        color=palette["hist"],
        edgecolor=palette["panel_bg"],
        alpha=0.85,
        ax=ax_returns,
    )
    ax_returns.axvline(0, color=palette["accent"], linewidth=1.2, alpha=0.85)
    ax_returns.axvline(
        mean_weekly_return,
        color=palette["ma"],
        linewidth=1.4,
        linestyle="--",
        alpha=0.9,
    )
    ax_returns.set_title("Weekly Return Distribution")
    ax_returns.set_xlabel("Weekly return")
    ax_returns.set_ylabel("Frequency")

    ax_news.bar(
        news_df["WeekEndingFriday"],
        news_df["NewsCount"],
        color=palette["news"],
        alpha=0.78,
        width=5,
    )
    ax_news.plot(
        news_df["WeekEndingFriday"],
        news_df["NewsCount"],
        color=palette["news_line"],
        linewidth=1.3,
        alpha=0.95,
    )
    ax_news.set_title("Weekly News Volume")
    ax_news.set_xlabel("Week ending")
    ax_news.set_ylabel("Articles")
    ax_news.xaxis.set_major_locator(mdates.YearLocator())
    ax_news.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))


    fig.suptitle(
        f"{company_label}",
        x=0.055,
        y=0.9,
        ha="left",
        va="top",
        fontsize=22,
        fontweight="bold",
        color=palette["text"],
    )

    fig.subplots_adjust(left=0.055, right=0.98, bottom=0.08, top=0.84)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    ticker = args.ticker.upper().strip()

    price_df = load_price_data(ticker)
    news_raw_df = load_news_data(ticker)
    news_weekly_df = aggregate_news_by_week(news_raw_df, price_df)
    limited_price_df, limited_news_df = limit_to_recent_weeks(
        price_df,
        news_weekly_df,
        args.weeks,
    )

    if limited_price_df.empty:
        raise ValueError(f"No price data available for ticker {ticker}.")
    if limited_news_df.empty:
        raise ValueError(f"No aligned news data available for ticker {ticker}.")

    company_label = get_company_label(ticker)
    output_path = (
        args.output
        if args.output is not None
        else OUTPUT_DIR / f"{ticker.lower()}_market_news_dashboard.png"
    )

    build_dashboard(
        company_label=company_label,
        price_df=limited_price_df,
        news_df=limited_news_df,
        output_path=output_path,
    )

    print(f"Dashboard saved to: {output_path.resolve()}")

    if args.show:
        saved_image = plt.imread(output_path)
        plt.figure(figsize=(16, 9))
        plt.imshow(saved_image)
        plt.axis("off")
        plt.show()


if __name__ == "__main__":
    main()
