import argparse
import sys
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config as cfg


OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
SECTOR_CODE_COLUMN = "SectorCode"
SECTOR_NAME_COLUMN = "Sector"
DATE_COLUMN = "WeekEndingFriday"
FIGURE_SIZE_INCHES = (16, 9)
FIGURE_DPI = 120

SECTOR_CODE_TO_NAME = {
    1: "Basic Materials",
    2: "Communication Services",
    3: "Consumer Cyclical",
    4: "Consumer Defensive",
    5: "Energy",
    6: "Financial Services",
    7: "Healthcare",
    8: "Industrials",
    9: "Real Estate",
    10: "Technology",
    11: "Utilities",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a slide-ready dashboard comparing sector and weekly "
            "observation distributions before and after the missing-value "
            "removal that produces modeling.csv."
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional custom output path for the PNG file.",
    )
    parser.add_argument(
        "--summary-output",
        type=Path,
        default=None,
        help="Optional custom output path for the sector summary CSV.",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Display the figure interactively after saving it.",
    )
    return parser.parse_args()


def apply_slide_theme() -> dict[str, object]:
    palette = {
        "figure_bg": "#06111f",
        "panel_bg": "#06111f",
        "text": "#edf5ff",
        "muted": "#9fb6cf",
        "accent": "#5cc8ff",
        "before": "#7189a8",
        "after": "#56cfe1",
        "loss": "#ff6b7a",
        "grid": "#28435f",
        "sector_colors": [
            "#4cc9f0",
            "#f8961e",
            "#90be6d",
            "#f94144",
            "#43aa8b",
            "#f9c74f",
            "#b5179e",
            "#ff8fab",
            "#b08968",
            "#adb5bd",
            "#277da1",
        ],
    }

    sns.set_theme(style="white")
    plt.rcParams.update(
        {
            "figure.facecolor": palette["figure_bg"],
            "axes.facecolor": palette["panel_bg"],
            "axes.edgecolor": palette["panel_bg"],
            "axes.labelcolor": palette["text"],
            "axes.titlecolor": palette["text"],
            "xtick.color": palette["muted"],
            "ytick.color": palette["muted"],
            "text.color": palette["text"],
            "savefig.facecolor": palette["figure_bg"],
            "savefig.edgecolor": palette["figure_bg"],
            "font.size": 11,
            "axes.titleweight": "bold",
        }
    )
    return palette


def load_sector_name_map() -> dict[int, str]:
    if not cfg.ENT.exists():
        return SECTOR_CODE_TO_NAME.copy()

    enterprises_df = pd.read_csv(cfg.ENT, usecols=["sector", SECTOR_CODE_COLUMN])
    enterprises_df = enterprises_df.dropna(subset=["sector", SECTOR_CODE_COLUMN]).copy()
    enterprises_df[SECTOR_CODE_COLUMN] = pd.to_numeric(
        enterprises_df[SECTOR_CODE_COLUMN], errors="coerce"
    ).astype("Int64")
    enterprises_df = enterprises_df.dropna(subset=[SECTOR_CODE_COLUMN])

    sector_name_map = SECTOR_CODE_TO_NAME.copy()
    for _, row in enterprises_df.drop_duplicates(SECTOR_CODE_COLUMN).iterrows():
        sector_name_map[int(row[SECTOR_CODE_COLUMN])] = str(row["sector"]).strip()
    return sector_name_map


def load_sector_counts(path: Path, sector_name_map: dict[int, str]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    df = pd.read_csv(path, usecols=[SECTOR_CODE_COLUMN])
    if df.empty:
        raise ValueError(f"Dataset is empty: {path}")

    df[SECTOR_CODE_COLUMN] = pd.to_numeric(df[SECTOR_CODE_COLUMN], errors="coerce")
    df = df.dropna(subset=[SECTOR_CODE_COLUMN]).copy()
    df[SECTOR_CODE_COLUMN] = df[SECTOR_CODE_COLUMN].astype(int)

    counts_df = (
        df.groupby(SECTOR_CODE_COLUMN, as_index=False)
        .size()
        .rename(columns={"size": "Observations"})
    )
    counts_df[SECTOR_NAME_COLUMN] = counts_df[SECTOR_CODE_COLUMN].map(sector_name_map)
    counts_df[SECTOR_NAME_COLUMN] = counts_df[SECTOR_NAME_COLUMN].fillna(
        "Sector " + counts_df[SECTOR_CODE_COLUMN].astype(str)
    )
    return counts_df.sort_values(SECTOR_CODE_COLUMN).reset_index(drop=True)


def build_summary_table(
    before_counts: pd.DataFrame,
    after_counts: pd.DataFrame,
) -> pd.DataFrame:
    summary_df = before_counts.merge(
        after_counts,
        on=[SECTOR_CODE_COLUMN, SECTOR_NAME_COLUMN],
        how="outer",
        suffixes=("_Before", "_After"),
    ).fillna(0)

    summary_df["Observations_Before"] = summary_df["Observations_Before"].astype(int)
    summary_df["Observations_After"] = summary_df["Observations_After"].astype(int)
    summary_df["Removed"] = (
        summary_df["Observations_Before"] - summary_df["Observations_After"]
    )
    summary_df["RetentionRate"] = (
        summary_df["Observations_After"] / summary_df["Observations_Before"]
    ).where(summary_df["Observations_Before"].ne(0), 0)

    total_before = summary_df["Observations_Before"].sum()
    total_after = summary_df["Observations_After"].sum()
    summary_df["Share_Before"] = summary_df["Observations_Before"] / total_before
    summary_df["Share_After"] = summary_df["Observations_After"] / total_after

    return summary_df.sort_values(SECTOR_CODE_COLUMN).reset_index(drop=True)


def load_weekly_counts(path: Path, output_column: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    df = pd.read_csv(path, usecols=[DATE_COLUMN])
    if df.empty:
        raise ValueError(f"Dataset is empty: {path}")

    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], errors="coerce")
    df = df.dropna(subset=[DATE_COLUMN]).copy()

    return (
        df.groupby(DATE_COLUMN, as_index=False)
        .size()
        .rename(columns={"size": output_column})
        .sort_values(DATE_COLUMN)
        .reset_index(drop=True)
    )


def build_weekly_summary() -> pd.DataFrame:
    before_weekly = load_weekly_counts(cfg.FULL_DATA, "Observations_Before")
    after_weekly = load_weekly_counts(cfg.MODELING_DATASET, "Observations_After")

    weekly_df = before_weekly.merge(after_weekly, on=DATE_COLUMN, how="outer")
    weekly_df[["Observations_Before", "Observations_After"]] = weekly_df[
        ["Observations_Before", "Observations_After"]
    ].fillna(0)
    weekly_df["Observations_Before"] = weekly_df["Observations_Before"].astype(int)
    weekly_df["Observations_After"] = weekly_df["Observations_After"].astype(int)
    weekly_df["Removed"] = (
        weekly_df["Observations_Before"] - weekly_df["Observations_After"]
    )
    weekly_df["Before_Percent"] = 100.0
    weekly_df["After_Percent"] = (
        weekly_df["Observations_After"] / weekly_df["Observations_Before"] * 100
    ).where(weekly_df["Observations_Before"].ne(0), 0)
    return weekly_df.sort_values(DATE_COLUMN).reset_index(drop=True)


def autopct_with_minimum(values: pd.Series):
    total = values.sum()

    def formatter(percent: float) -> str:
        count = int(round(percent * total / 100))
        if percent < 4:
            return ""
        return f"{percent:.1f}%\n{count:,}"

    return formatter


def draw_pie(
    ax: plt.Axes,
    values: pd.Series,
    title: str,
    colors: list[str],
    palette: dict[str, object],
) -> None:
    wedges, _, autotexts = ax.pie(
        values,
        labels=None,
        colors=colors,
        startangle=90,
        counterclock=False,
        wedgeprops={"linewidth": 1.2, "edgecolor": palette["figure_bg"]},
        autopct=autopct_with_minimum(values),
        pctdistance=0.72,
        radius=1.15,
        textprops={"fontsize": 11, "fontweight": "bold", "color": palette["text"]},
    )
    ax.set_title(title, fontsize=16, pad=-4)
    ax.axis("equal")
    ax.set_facecolor(palette["figure_bg"])

    for text in autotexts:
        text.set_bbox(
            {
                "boxstyle": "round,pad=0.25",
                "facecolor": palette["figure_bg"],
                "edgecolor": "none",
                "alpha": 0.92,
            }
        )

    return wedges


def draw_weekly_histogram(
    ax: plt.Axes,
    weekly_df: pd.DataFrame,
    palette: dict[str, object],
) -> None:
    week_dates = weekly_df[DATE_COLUMN]

    ax.bar(
        week_dates,
        weekly_df["Before_Percent"],
        width=5.6,
        color=palette["before"],
        alpha=0.42,
        edgecolor=palette["before"],
        linewidth=0,
        antialiased=False,
        label="Before cleaning",
        zorder=1,
    )
    ax.bar(
        week_dates,
        weekly_df["After_Percent"],
        width=5.6,
        color=palette["after"],
        alpha=0.95,
        edgecolor=palette["after"],
        linewidth=0,
        antialiased=False,
        label="After cleaning",
        zorder=2,
    )

    ax.set_title("Weekly Observation Retention", fontsize=21, pad=14, fontweight="bold")
    ax.set_ylabel("Share of before-cleaning observations", fontsize=13, color=palette["text"])
    ax.set_xlabel("Week ending", fontsize=13, color=palette["text"])
    ax.set_facecolor(palette["figure_bg"])
    ax.grid(axis="y", color=palette["grid"], alpha=0.55, linewidth=0.8)
    ax.grid(axis="x", visible=False)
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(palette["grid"])
    ax.tick_params(colors=palette["muted"], labelsize=11)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.set_ylim(0, 112)
    ax.yaxis.set_major_formatter(lambda value, _: f"{value:.0f}%")

    legend = ax.legend(
        loc="lower right",
        bbox_to_anchor=(1.0, 1.03),
        frameon=False,
        fontsize=13,
        labelcolor=palette["text"],
        ncol=2,
    )
    for text in legend.get_texts():
        text.set_color(palette["text"])


def create_figure(
    summary_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    output_path: Path,
    show: bool,
) -> None:
    palette = apply_slide_theme()
    sector_colors = palette["sector_colors"]

    before_values = summary_df["Observations_Before"]
    after_values = summary_df["Observations_After"]
    total_before = int(before_values.sum())
    total_after = int(after_values.sum())
    removed = total_before - total_after
    retention = total_after / total_before

    fig = plt.figure(figsize=FIGURE_SIZE_INCHES, dpi=FIGURE_DPI)
    before_ax = fig.add_axes([0.055, 0.43, 0.34, 0.42])
    after_ax = fig.add_axes([0.40, 0.43, 0.34, 0.42])
    weekly_ax = fig.add_axes([0.065, 0.145, 0.87, 0.22])
    axes = [before_ax, after_ax]

    draw_pie(
        axes[0],
        before_values,
        "Before Cleaning",
        sector_colors,
        palette,
    )
    wedges = draw_pie(
        axes[1],
        after_values,
        "After Cleaning",
        sector_colors,
        palette,
    )
    draw_weekly_histogram(weekly_ax, weekly_df, palette)

    fig.suptitle(
        "Observation Distribution Before and After Cleaning",
        fontsize=28,
        fontweight="bold",
        y=0.965,
    )

    legend_labels = summary_df[SECTOR_NAME_COLUMN].tolist()
    fig.legend(
        wedges,
        legend_labels,
        title="Sector",
        loc="center left",
        bbox_to_anchor=(0.735, 0.64),
        frameon=False,
        fontsize=12,
        title_fontsize=14,
        labelcolor=palette["text"],
        labelspacing=0.42,
        handlelength=1.6,
        handletextpad=0.6,
    )
    legend = fig.legends[0]
    legend.get_title().set_color(palette["text"])

    fig.text(
        0.065,
        0.065,
        f"Rows removed: {removed:,} ({1 - retention:.1%} of the initial dataset)",
        fontsize=18,
        fontweight="bold",
        color=palette["loss"],
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    if show:
        plt.show()
    plt.close(fig)


def main() -> None:
    args = parse_args()
    output_path = args.output or OUTPUT_DIR / "sector_distribution_before_after_modeling.png"
    summary_output_path = (
        args.summary_output
        or OUTPUT_DIR / "sector_distribution_before_after_modeling.csv"
    )

    sector_name_map = load_sector_name_map()
    before_counts = load_sector_counts(cfg.FULL_DATA, sector_name_map)
    after_counts = load_sector_counts(cfg.MODELING_DATASET, sector_name_map)
    summary_df = build_summary_table(before_counts, after_counts)
    weekly_df = build_weekly_summary()

    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(summary_output_path, index=False, encoding="utf-8-sig")
    create_figure(summary_df, weekly_df, output_path, args.show)

    print(
        "Sector distribution chart created:",
        {
            "before_dataset": str(cfg.FULL_DATA),
            "after_dataset": str(cfg.MODELING_DATASET),
            "before_rows": int(summary_df["Observations_Before"].sum()),
            "after_rows": int(summary_df["Observations_After"].sum()),
            "removed_rows": int(summary_df["Removed"].sum()),
            "weekly_points": len(weekly_df),
            "figure_output": str(output_path),
            "summary_output": str(summary_output_path),
        },
    )


if __name__ == "__main__":
    main()
