import argparse
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Rectangle

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config as cfg


OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
SIDE_MARGIN_LEFT = 0.14
SIDE_MARGIN_RIGHT = 0.07
CARD_GAP = 0.015
CARD_COUNT = 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a slide-ready dashboard with repository effort and GitHub "
            "project statistics."
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


def run_git_command(args: list[str]) -> str:
    return subprocess.check_output(args, cwd=cfg.ROOT, text=True, encoding="utf-8").strip()


def apply_dark_theme() -> dict[str, str]:
    palette = {
        "figure_bg": "#06111a",
        "panel_bg": "#101d2b",
        "panel_alt": "#152637",
        "grid": "#30485f",
        "outline": "#42627f",
        "text": "#edf2f7",
        "muted": "#9cb0c4",
        "accent": "#53d1f0",
        "accent_2": "#ffd166",
        "accent_3": "#7bd389",
        "accent_4": "#ef476f",
        "accent_5": "#b388eb",
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
        }
    )
    return palette


def add_card(
    fig,
    x: float,
    y: float,
    w: float,
    h: float,
    title: str,
    value: str,
    subtitle: str,
    palette: dict[str, str],
) -> None:
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
    line_y = y + h * 0.64
    subtitle_y = y + 0.012
    title_x = x + 0.014
    value_x = x + w * 0.48

    fig.text(
        title_x,
        line_y,
        title,
        color=palette["text"],
        fontsize=15,
        fontweight="bold",
        va="center",
    )
    fig.text(
        value_x,
        line_y,
        value,
        color=palette["text"],
        fontsize=22,
        fontweight="bold",
        va="center",
    )
    fig.text(
        x + 0.014,
        subtitle_y,
        subtitle,
        color=palette["muted"],
        fontsize=10.5,
        va="bottom",
    )


def collect_python_code_metrics() -> tuple[pd.DataFrame, int, int]:
    rows = []
    total_loc = 0
    total_files = 0

    for path in sorted(cfg.SRC.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue

        relative_parts = path.relative_to(cfg.SRC).parts
        area = relative_parts[0]
        if area == "config.py":
            area = "shared config"

        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            loc = sum(1 for _ in handle)

        rows.append({"Area": area, "File": str(path), "LOC": loc})
        total_loc += loc
        total_files += 1

    code_df = pd.DataFrame(rows)
    loc_by_area = (
        code_df.groupby("Area", as_index=False)
        .agg(LOC=("LOC", "sum"), Files=("File", "count"))
        .sort_values("LOC", ascending=True)
    )
    return loc_by_area, total_loc, total_files


def collect_git_metrics() -> tuple[int, int, pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    total_commits = int(run_git_command(["git", "rev-list", "--count", "HEAD"]))

    date_lines = run_git_command(
        ["git", "log", "--date=format:%Y-%m-%d", "--pretty=format:%ad"]
    ).splitlines()
    daily_counts = pd.Series(date_lines).value_counts().sort_index()
    daily_counts.index = pd.to_datetime(daily_counts.index)
    active_days = int(daily_counts.shape[0])

    full_index = pd.date_range(daily_counts.index.min(), daily_counts.index.max(), freq="D")
    daily_df = (
        daily_counts.reindex(full_index, fill_value=0)
        .rename_axis("Date")
        .reset_index(name="Commits")
    )
    daily_df["CumulativeCommits"] = daily_df["Commits"].cumsum()

    start_date = pd.to_datetime(daily_df["Date"].min())
    end_date = pd.to_datetime(daily_df["Date"].max())
    return total_commits, active_days, daily_df, start_date, end_date


def collect_company_universe_metrics() -> pd.DataFrame:
    selected_companies = len(list(cfg.RAW_NEWS_DATA.glob("*.csv")))
    universe_df = pd.read_csv(cfg.COMPANY_SELECTION_UNIVERSE)
    total_possible_companies = len(universe_df)
    total_csv_datasets = sum(1 for path in cfg.DATA.rglob("*.csv") if path.is_file())
    return pd.DataFrame(
        [
            {"Artifact": "Selected companies", "Count": selected_companies},
            {"Artifact": "Total analyzed companies", "Count": total_possible_companies},
            {"Artifact": "Total CSV datasets", "Count": total_csv_datasets},
        ]
    )


def format_date_range(start_date: pd.Timestamp, end_date: pd.Timestamp) -> str:
    return f"{start_date.strftime('%b %Y')} - {end_date.strftime('%b %Y')}"


def build_dashboard(output_path: Path) -> None:
    palette = apply_dark_theme()
    usable_width = 1 - SIDE_MARGIN_LEFT - SIDE_MARGIN_RIGHT
    card_width = (usable_width - CARD_GAP * (CARD_COUNT - 1)) / CARD_COUNT

    loc_by_area, total_loc, total_py_files = collect_python_code_metrics()
    total_commits, active_days, daily_df, start_date, end_date = collect_git_metrics()
    assets_df = collect_company_universe_metrics()

    fig = plt.figure(figsize=(16, 9), dpi=160)
    fig.patch.set_facecolor(palette["figure_bg"])
    grid = GridSpec(
        2,
        2,
        figure=fig,
        left=SIDE_MARGIN_LEFT,
        right=1 - SIDE_MARGIN_RIGHT,
        bottom=0.08,
        top=0.76,
        hspace=0.26,
        wspace=0.18,
        height_ratios=[1.05, 1.0],
    )

    ax_loc = fig.add_subplot(grid[0, :])
    ax_assets = fig.add_subplot(grid[1, 0])
    ax_timeline = fig.add_subplot(grid[1, 1])

    for axis in (ax_loc, ax_assets, ax_timeline):
        axis.set_facecolor(palette["panel_bg"])
        for spine in axis.spines.values():
            spine.set_color(palette["outline"])

    fig.suptitle(
        "Repository statistics",
        x=0.5,
        y=0.965,
        ha="center",
        fontsize=35,
        fontweight="bold",
    )

    add_card(
        fig,
        SIDE_MARGIN_LEFT + (card_width + CARD_GAP) * 0,
        0.79,
        card_width,
        0.095,
        "Total commits",
        f"{total_commits:,}",
        f"Across {format_date_range(start_date, end_date)}",
        palette,
    )
    add_card(
        fig,
        SIDE_MARGIN_LEFT + (card_width + CARD_GAP) * 1,
        0.79,
        card_width,
        0.095,
        "Active days",
        f"{active_days:,}",
        "Distinct days with at least one commit",
        palette,
    )
    add_card(
        fig,
        SIDE_MARGIN_LEFT + (card_width + CARD_GAP) * 2,
        0.79,
        card_width,
        0.095,
        "Python rows",
        f"{total_loc:,}",
        f"Across {total_py_files} Python files",
        palette,
    )

    ax_loc.barh(
        loc_by_area["Area"],
        loc_by_area["LOC"],
        color=palette["accent"],
        alpha=0.92,
    )
    ax_loc.set_title("Python Code Footprint By Project Area")
    ax_loc.set_xlabel("Python rows")
    ax_loc.set_ylabel("")
    max_loc = loc_by_area["LOC"].max()
    for _, row in loc_by_area.iterrows():
        padding = max_loc * 0.005
        label_x = max(row["LOC"] - padding, row["LOC"] * 0.45)
        font_size = 11.5 if row["LOC"] >= max_loc * 0.20 else 9.5
        ax_loc.text(
            label_x,
            row["Area"],
            f"{int(row['LOC']):,}",
            va="center",
            ha="right",
            color=palette["panel_bg"],
            fontsize=font_size,
            fontweight="bold",
        )

    asset_colors = [palette["accent"], palette["accent_2"], palette["accent_4"]]
    ax_assets.barh(
        assets_df["Artifact"],
        assets_df["Count"],
        color=asset_colors,
        alpha=0.92,
    )
    ax_assets.set_title("Number of companies and datasets analyzed")
    ax_assets.set_xlabel("count")
    ax_assets.set_ylabel("")
    max_asset_count = assets_df["Count"].max()
    ax_assets.set_xlim(0, max_asset_count * 1.08)
    for _, row in assets_df.iterrows():
        padding = max_asset_count * 0.05
        label_x = max(row["Count"] - padding, row["Count"] * 0.45)
        font_size = 12 if row["Count"] >= max_asset_count * 0.20 else 10
        ax_assets.text(
            label_x,
            row["Artifact"],
            f"{int(row['Count']):,}",
            va="center",
            ha="right",
            color=palette["panel_bg"],
            fontsize=font_size,
            fontweight="bold",
        )

    ax_timeline.plot(
        daily_df["Date"],
        daily_df["CumulativeCommits"],
        color=palette["accent_2"],
        linewidth=2.8,
    )
    ax_timeline.fill_between(
        daily_df["Date"],
        daily_df["CumulativeCommits"],
        color=palette["accent_2"],
        alpha=0.12,
    )
    ax_timeline.bar(
        daily_df["Date"],
        daily_df["Commits"],
        color=palette["accent_4"],
        alpha=0.35,
        width=1.0,
    )
    ax_timeline.set_title("Commit Timeline")
    ax_timeline.set_ylabel("Cumulative commits")
    ax_timeline.set_xlabel("Date")
    ax_timeline.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    ax_timeline.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax_timeline.tick_params(axis="x", rotation=35)
    ax_timeline.text(
        0.02,
        0.88,
        f"{daily_df['Commits'].max()} commits on the busiest day",
        transform=ax_timeline.transAxes,
        color=palette["text"],
        fontsize=10,
        bbox={"facecolor": palette["panel_alt"], "edgecolor": palette["outline"], "pad": 6},
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    output_path = (
        args.output
        if args.output is not None
        else OUTPUT_DIR / "repo_effort_dashboard.png"
    )

    build_dashboard(output_path=output_path)
    print(f"Dashboard saved to: {output_path.resolve()}")

    if args.show:
        saved_image = plt.imread(output_path)
        plt.figure(figsize=(16, 9))
        plt.imshow(saved_image)
        plt.axis("off")
        plt.show()


if __name__ == "__main__":
    main()
