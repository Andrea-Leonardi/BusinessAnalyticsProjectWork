import argparse
import html
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Rectangle

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config as cfg


OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
MIN_SUMMARY_WORDS = 5
MIN_SUMMARY_CHARS = 25
HIGH_TITLE_SIMILARITY = 0.92
SIDE_MARGIN = 0.15
CARD_WIDTH = 0.162
CARD_GAP = 0.012
CARD_HEIGHT = 0.115

BENZINGA_BAD_TEXT_PATTERNS = [
    r"headline only article",
    r"benzinga pro traders",
    r"benzinga does not provide investment advice",
    r"all rights reserved",
    r"to add benzinga news as your preferred source on google",
    r"never miss a trade again",
]
LOW_QUALITY_FMP_SUMMARIES = {
    "benzinga",
    "bloomberg",
    "reuters",
    "upgrades",
    "downgrades",
    "the",
    "u.s.",
    "us",
}
LOW_QUALITY_FMP_PREFIXES = (
    "according to",
    "shares of",
    "the stock",
    "stocks of",
)
LOW_QUALITY_FMP_SUFFIXES = (
    "reported",
    "according to",
    "after a report emerged that",
    "following a report that",
    "following report",
    "amid reports",
    "on report",
)

ISSUE_ORDER = [
    "missing",
    "too_short",
    "generic_source_only",
    "boilerplate",
    "generic_feed_phrase",
    "truncated",
    "headline_like",
    "usable",
]
ISSUE_LABELS = {
    "missing": "Missing / empty",
    "too_short": "Too short",
    "generic_source_only": "Generic source-only",
    "boilerplate": "Boilerplate / promo",
    "generic_feed_phrase": "Generic feed phrase",
    "truncated": "Truncated",
    "headline_like": "Almost identical to headline",
    "usable": "Usable summary",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a slide-ready dashboard that explains why raw news "
            "summaries had poor quality and how missingSummaryImputation fixed them."
        )
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


def normalize_text(text: object) -> str:
    if pd.isna(text) or not isinstance(text, str):
        return ""

    cleaned = html.unescape(text)
    cleaned = cleaned.replace("\xa0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def normalize_text_for_comparison(text: object) -> str:
    cleaned = normalize_text(text)
    if not cleaned:
        return ""

    cleaned = re.sub(r"http\S+|www\S+|https\S+", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"\@\w+|\#", "", cleaned)
    cleaned = re.sub(r"[^a-zA-Z\s]", " ", cleaned)
    cleaned = cleaned.lower()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def classify_summary_issue(summary: object, headline: object) -> str:
    cleaned_summary = normalize_text(summary)
    canonical_summary = normalize_text_for_comparison(cleaned_summary)
    canonical_headline = normalize_text_for_comparison(headline)

    if not cleaned_summary:
        return "missing"

    summary_words = canonical_summary.split()

    if len(cleaned_summary) < MIN_SUMMARY_CHARS or len(summary_words) < MIN_SUMMARY_WORDS:
        return "too_short"

    if canonical_summary in LOW_QUALITY_FMP_SUMMARIES:
        return "generic_source_only"

    if any(
        re.search(pattern, cleaned_summary, flags=re.IGNORECASE)
        for pattern in BENZINGA_BAD_TEXT_PATTERNS
    ):
        return "boilerplate"

    if canonical_summary.startswith(LOW_QUALITY_FMP_PREFIXES) and len(summary_words) <= 9:
        return "generic_feed_phrase"
    if canonical_summary.endswith(LOW_QUALITY_FMP_SUFFIXES) and len(summary_words) <= 12:
        return "generic_feed_phrase"

    if cleaned_summary.endswith("...") or cleaned_summary.endswith("â€¦"):
        return "truncated"
    if cleaned_summary[-1].isalnum() and len(summary_words) >= 6 and len(summary_words[-1]) <= 4:
        return "truncated"

    if canonical_headline:
        similarity = SequenceMatcher(None, canonical_summary, canonical_headline).ratio()
        if similarity >= HIGH_TITLE_SIMILARITY:
            return "headline_like"

        headline_tokens = set(canonical_headline.split())
        summary_tokens = set(summary_words)
        if summary_tokens and summary_tokens.issubset(headline_tokens) and len(summary_words) <= 10:
            return "headline_like"

    return "usable"


def load_raw_news() -> pd.DataFrame:
    raw_paths = sorted(cfg.RAW_NEWS_DATA.glob("*.csv"))
    if not raw_paths:
        raise FileNotFoundError(f"No raw news CSV files found in: {cfg.RAW_NEWS_DATA}")

    raw_df = pd.concat((pd.read_csv(path) for path in raw_paths), ignore_index=True)
    if raw_df.empty:
        raise ValueError("The raw news dataset is empty.")

    raw_df["Headline"] = raw_df["Headline"].apply(normalize_text)
    raw_df["Summary"] = raw_df["Summary"].apply(normalize_text)
    raw_df["SummaryWords"] = raw_df["Summary"].apply(
        lambda text: len(normalize_text_for_comparison(text).split())
    )
    raw_df["SummaryChars"] = raw_df["Summary"].str.len()
    raw_df["IssueType"] = raw_df.apply(
        lambda row: classify_summary_issue(row["Summary"], row["Headline"]),
        axis=1,
    )
    raw_df["NeedsHeadlineFallback"] = raw_df["IssueType"] != "usable"
    return raw_df


def load_clean_news() -> pd.DataFrame:
    if not cfg.NEWS_ARTICLES.exists():
        raise FileNotFoundError(f"Cleaned news dataset not found: {cfg.NEWS_ARTICLES}")

    clean_df = pd.read_csv(cfg.NEWS_ARTICLES)
    clean_df["Headline"] = clean_df["Headline"].apply(normalize_text)
    clean_df["Summary"] = clean_df["Summary"].apply(normalize_text)
    return clean_df


def apply_dark_theme() -> dict[str, str]:
    palette = {
        "figure_bg": "#071018",
        "panel_bg": "#101c2a",
        "panel_alt": "#142334",
        "grid": "#30475f",
        "text": "#edf2f7",
        "muted": "#9eb0c2",
        "good": "#72c472",
        "missing": "#ff6b6b",
        "weak": "#ff9f43",
        "generic": "#7d8cff",
        "boilerplate": "#ef476f",
        "headline": "#48cae4",
        "accent": "#ffd166",
        "outline": "#40617f",
    }

    sns.set_theme(style="darkgrid")
    plt.rcParams.update(
        {
            "figure.facecolor": palette["figure_bg"],
            "axes.facecolor": palette["panel_bg"],
            "axes.edgecolor": palette["outline"],
            "axes.labelcolor": palette["text"],
            "axes.titlecolor": palette["text"],
            "xtick.color": palette["muted"],
            "ytick.color": palette["muted"],
            "grid.color": palette["grid"],
            "grid.alpha": 0.25,
            "savefig.facecolor": palette["figure_bg"],
            "savefig.edgecolor": palette["figure_bg"],
            "text.color": palette["text"],
            "font.size": 11,
            "axes.titleweight": "bold",
            "axes.titlesize": 19,
        }
    )
    return palette


def add_card(fig, x: float, y: float, w: float, h: float, title: str, value: str, subtitle: str, palette: dict[str, str]) -> None:
    rect = Rectangle(
        (x, y),
        w,
        h,
        transform=fig.transFigure,
        facecolor=palette["panel_alt"],
        edgecolor=palette["outline"],
        linewidth=1.2,
    )
    fig.add_artist(rect)
    fig.text(
        x + 0.015,
        y + h - 0.032,
        title,
        color=palette["text"],
        fontsize=17,
        fontweight="bold",
    )
    fig.text(
        x + 0.015,
        y + 0.025,
        value,
        color=palette["text"],
        fontsize=22,
        fontweight="bold",
    )


def format_pct(count: int, total: int) -> str:
    return f"{(100 * count / total):.1f}%"


def build_issue_colors(palette: dict[str, str]) -> dict[str, str]:
    return {
        "missing": palette["missing"],
        "too_short": palette["weak"],
        "generic_source_only": palette["generic"],
        "boilerplate": palette["boilerplate"],
        "generic_feed_phrase": "#b388eb",
        "truncated": palette["accent"],
        "headline_like": palette["headline"],
        "usable": palette["good"],
    }


def build_figure(raw_df: pd.DataFrame, clean_df: pd.DataFrame, output_path: Path) -> None:
    palette = apply_dark_theme()
    issue_colors = build_issue_colors(palette)

    total_rows = len(raw_df)
    issue_counts = raw_df["IssueType"].value_counts().reindex(ISSUE_ORDER, fill_value=0)
    low_quality_count = int(issue_counts.drop(labels=["missing", "usable"]).sum())
    missing_count = int(issue_counts["missing"])
    usable_before_count = int(issue_counts["usable"])
    fallback_count = int(raw_df["NeedsHeadlineFallback"].sum())
    clean_non_empty_count = int(clean_df["Summary"].ne("").sum())

    top_issues = (
        issue_counts.drop(labels=["usable"])
        .sort_values(ascending=False)
        .head(5)
        .rename(index=ISSUE_LABELS)
    )
    cards_total_width = CARD_WIDTH * 5 + CARD_GAP * 4
    cards_left = (1 - cards_total_width) / 2

    fig = plt.figure(figsize=(16, 9), dpi=160)
    fig.patch.set_facecolor(palette["figure_bg"])
    grid = GridSpec(
        2,
        1,
        figure=fig,
        left=SIDE_MARGIN,
        right=1 - SIDE_MARGIN,
        bottom=0.09,
        top=0.73,
        hspace=0.28,
        height_ratios=[1.2, 1.0],
    )

    ax_breakdown = fig.add_subplot(grid[0, 0])
    ax_causes = fig.add_subplot(grid[1, 0])

    for axis in (ax_breakdown, ax_causes):
        axis.set_facecolor(palette["panel_bg"])
        for spine in axis.spines.values():
            spine.set_color(palette["outline"])

    fig.suptitle(
        "News Summary Quality Was a Real Data Problem",
        x=cards_left,
        y=0.96,
        ha="left",
        fontsize=24,
        fontweight="bold",
    )
    add_card(
        fig,
        cards_left,
        0.79,
        CARD_WIDTH,
        CARD_HEIGHT,
        "Raw articles",
        f"{total_rows:,}",
        "Rows scanned across the full news dataset",
        palette,
    )
    add_card(
        fig,
        cards_left + (CARD_WIDTH + CARD_GAP) * 1,
        0.79,
        CARD_WIDTH,
        CARD_HEIGHT,
        "Empty summaries",
        f"{missing_count:,}",
        f"{format_pct(missing_count, total_rows)} were empty before cleaning",
        palette,
    )
    add_card(
        fig,
        cards_left + (CARD_WIDTH + CARD_GAP) * 2,
        0.79,
        CARD_WIDTH,
        CARD_HEIGHT,
        "Low-quality",
        f"{low_quality_count:,}",
        "Present, but judged unreliable by heuristics",
        palette,
    )
    add_card(
        fig,
        cards_left + (CARD_WIDTH + CARD_GAP) * 3,
        0.79,
        CARD_WIDTH,
        CARD_HEIGHT,
        "Headline fallback",
        f"{fallback_count:,}",
        f"{format_pct(fallback_count, total_rows)} replaced by headline",
        palette,
    )
    add_card(
        fig,
        cards_left + (CARD_WIDTH + CARD_GAP) * 4,
        0.79,
        CARD_WIDTH,
        CARD_HEIGHT,
        "After cleaning",
        f"{clean_non_empty_count:,}",
        "Articles with non-empty summary text in newsArticles.csv",
        palette,
    )

    break_pos = ax_breakdown.get_position()
    shorter_height = break_pos.height * 0.62
    centered_bottom = break_pos.y0 + (break_pos.height - shorter_height) / 2
    ax_breakdown.set_position(
        [break_pos.x0, centered_bottom, break_pos.width, shorter_height]
    )

    segments = issue_counts.drop(labels=["usable"])
    left = 0
    for issue, count in segments.items():
        if count == 0:
            continue
        ax_breakdown.barh(
            ["Raw summaries"],
            [count],
            left=left,
            color=issue_colors[issue],
            edgecolor=palette["panel_bg"],
            height=0.5,
        )
        midpoint = left + count / 2
        if count / total_rows >= 0.045:
            ax_breakdown.text(
                midpoint,
                0,
                f"{ISSUE_LABELS[issue]}\n{format_pct(int(count), total_rows)}",
                ha="center",
                va="center",
                fontsize=18,
                color=palette["text"],
                fontweight="bold",
            )
        left += count

    ax_breakdown.barh(
        ["Raw summaries"],
        [usable_before_count],
        left=left,
        color=issue_colors["usable"],
        edgecolor=palette["panel_bg"],
        height=0.5,
    )
    ax_breakdown.text(
        left + usable_before_count / 2,
        0,
        f"Usable\n{format_pct(usable_before_count, total_rows)}",
        ha="center",
        va="center",
        fontsize=18,
        color=palette["text"],
        fontweight="bold",
    )
    ax_breakdown.set_title(
        "Before Cleaning: How Raw Summaries Were Split",
        fontsize=20,
        pad=16,
    )
    ax_breakdown.set_xlabel("Number of articles")
    ax_breakdown.set_ylabel("")
    ax_breakdown.grid(False)

    ax_causes.barh(
        top_issues.index[::-1],
        top_issues.values[::-1],
        color=[issue_colors[key] for key in issue_counts.drop(labels=["usable"]).sort_values(ascending=False).head(5).index[::-1]],
        alpha=0.92,
    )
    ax_causes.set_title(
        "Main Sources of Bad Summaries",
        fontsize=20,
        pad=16,
    )
    ax_causes.set_xlabel("Articles")
    ax_causes.set_ylabel("")
    for idx, value in enumerate(top_issues.values[::-1]):
        ax_causes.text(
            value + total_rows * 0.003,
            idx,
            f"{value:,}",
            va="center",
            color=palette["muted"],
            fontsize=9,
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    raw_df = load_raw_news()
    clean_df = load_clean_news()
    output_path = (
        args.output
        if args.output is not None
        else OUTPUT_DIR / "news_summary_quality_story.png"
    )

    build_figure(raw_df=raw_df, clean_df=clean_df, output_path=output_path)
    print(f"Visualization saved to: {output_path.resolve()}")

    if args.show:
        saved_image = plt.imread(output_path)
        plt.figure(figsize=(16, 9))
        plt.imshow(saved_image)
        plt.axis("off")
        plt.show()


if __name__ == "__main__":
    main()
