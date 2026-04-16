#%%
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config as cfg

# ---------------------------------------------------------------------------
# Summarize Missing And Zero Values By Company Financial File
# ---------------------------------------------------------------------------

# Inspect each company-level processed financial file and count how many
# missing values and explicit zeros it contains. The binary release flag is
# excluded because its zeros are structural by construction.
financial_files = sorted(cfg.SINGLE_COMPANY_FINANCIALS.glob("*Financials.csv"))

quality_rows = []
if not financial_files:
    print("\nNo company financial files were found in the financial output folder.")
else:
    for financial_file in financial_files:
        company_df = pd.read_csv(financial_file)

        analysis_columns = [
            column for column in company_df.columns if column != "QuarterlyReleased"
        ]
        numeric_columns = [
            column
            for column in analysis_columns
            if pd.api.types.is_numeric_dtype(company_df[column])
        ]
        total_cells = len(company_df) * len(analysis_columns)
        total_numeric_cells = len(company_df) * len(numeric_columns)
        missing_values = int(company_df[analysis_columns].isna().sum().sum())
        zero_values = int(company_df[numeric_columns].eq(0).sum().sum())

        quality_rows.append(
            {
                "Ticker": financial_file.stem.replace("Financials", ""),
                "Rows": len(company_df),
                "MissingValues": missing_values,
                "MissingPct": (
                    100 * missing_values / total_cells if total_cells > 0 else 0.0
                ),
                "ZeroValues": zero_values,
                "ZeroPct": (
                    100 * zero_values / total_numeric_cells
                    if total_numeric_cells > 0
                    else 0.0
                ),
            }
        )

    quality_summary_df = pd.DataFrame(quality_rows).sort_values(
        by=["MissingValues", "ZeroValues", "Ticker"],
        ascending=[False, False, True],
    ).reset_index(drop=True)
    quality_summary_df["MissingPct"] = quality_summary_df["MissingPct"].round(2)
    quality_summary_df["ZeroPct"] = quality_summary_df["ZeroPct"].round(2)

    print("\nMissing and zero-value summary for company financial files:")
    print(quality_summary_df.to_string(index=False))

# %%


# ---------------------------------------------------------------------------
# Plot Correlations For The ML-Ready Dataset
# ---------------------------------------------------------------------------

# Load the final ML-ready dataset and keep only the numeric columns whose
# absolute correlation with at least one other variable is at least 0.20.
CORRELATION_THRESHOLD = 0.20

correlation_df = pd.DataFrame()
top_target_correlations = pd.Series(dtype="float64")

if not cfg.FULL_DATA_ML.exists():
    print(f"\nML-ready dataset not found: {cfg.FULL_DATA_ML}")
else:
    ml_df = pd.read_csv(cfg.FULL_DATA_ML)
    correlation_df = ml_df.select_dtypes(include="number")

    if correlation_df.empty:
        print("\nNo numeric columns were found in fulldata_ml.csv.")
    else:
        correlation_matrix = correlation_df.corr()

        relevant_correlation_mask = (
            correlation_matrix.abs().ge(CORRELATION_THRESHOLD)
            & ~pd.DataFrame(
                np.eye(len(correlation_matrix), dtype=bool),
                index=correlation_matrix.index,
                columns=correlation_matrix.columns,
            )
        )
        selected_columns = relevant_correlation_mask.any(axis=1)
        filtered_correlation_matrix = correlation_matrix.loc[
            selected_columns,
            selected_columns,
        ]

        if filtered_correlation_matrix.empty:
            print(
                "\nNo correlations with absolute value >= "
                f"{CORRELATION_THRESHOLD:.2f} were found in fulldata_ml.csv."
            )
        else:
            # Mask the upper triangle so the plot reads like a compact corrplot.
            upper_triangle_mask = np.triu(
                np.ones_like(filtered_correlation_matrix, dtype=bool)
            )

            plt.figure(figsize=(14, 10))
            sns.heatmap(
                filtered_correlation_matrix,
                mask=upper_triangle_mask,
                cmap="coolwarm",
                center=0,
                vmin=-1,
                vmax=1,
                linewidths=0.5,
                square=True,
                cbar_kws={"shrink": 0.8},
            )
            plt.title(
                "Correlation Plot For fulldata_ml.csv "
                f"(absolute correlation >= {CORRELATION_THRESHOLD:.2f})"
            )
            plt.xticks(rotation=45, ha="right")
            plt.yticks(rotation=0)
            plt.tight_layout()
            plt.show()

# %%
TARGET_COLUMN = "AdjClosePrice_t+1"
TOP_TARGET_FEATURES = 20


# ---------------------------------------------------------------------------
# Plot Target-Centric Correlations For The ML-Ready Dataset
# ---------------------------------------------------------------------------

# Focus on the relationships that are most relevant for prediction by looking
# only at the correlations between each feature and the selected target.
if TARGET_COLUMN not in correlation_df.columns:
    print(f"\nTarget column not found in fulldata_ml.csv: {TARGET_COLUMN}")
else:
    target_correlations = (
        correlation_df.corr()[TARGET_COLUMN]
        .drop(labels=[TARGET_COLUMN])
        .dropna()
        .sort_values(key=lambda series: series.abs(), ascending=False)
    )

    top_target_correlations = target_correlations.head(TOP_TARGET_FEATURES)

    if top_target_correlations.empty:
        print(
            f"\nNo valid feature correlations were available for {TARGET_COLUMN}."
        )
    else:
        plt.figure(figsize=(12, 8))
        sns.barplot(
            x=top_target_correlations.values,
            y=top_target_correlations.index,
            hue=top_target_correlations.index,
            dodge=False,
            palette="coolwarm",
            legend=False,
        )
        plt.axvline(0, color="black", linewidth=1)
        plt.title(
            f"Top {TOP_TARGET_FEATURES} Feature Correlations With {TARGET_COLUMN}"
        )
        plt.xlabel("Pearson Correlation")
        plt.ylabel("Feature")
        plt.tight_layout()
        plt.show()


# %%
TOP_HEATMAP_FEATURES = 15


# ---------------------------------------------------------------------------
# Plot A Compact Heatmap Around The Target
# ---------------------------------------------------------------------------

# Build a smaller heatmap around the features that are most correlated with
# the target, so multicollinearity is easier to inspect visually.
if TARGET_COLUMN not in correlation_df.columns:
    print(f"\nTarget column not found in fulldata_ml.csv: {TARGET_COLUMN}")
elif top_target_correlations.empty:
    print(
        f"\nNo compact target-centered heatmap was created for {TARGET_COLUMN}."
    )
else:
    heatmap_columns = [TARGET_COLUMN] + top_target_correlations.index[
        :TOP_HEATMAP_FEATURES
    ].tolist()
    compact_correlation_matrix = correlation_df[heatmap_columns].corr()

    plt.figure(figsize=(12, 10))
    sns.heatmap(
        compact_correlation_matrix,
        cmap="coolwarm",
        center=0,
        vmin=-1,
        vmax=1,
        annot=True,
        fmt=".2f",
        annot_kws={"size": 7},
        linewidths=0.5,
        square=True,
        cbar_kws={"shrink": 0.8},
    )
    plt.title(
        f"Compact Correlation Heatmap Around {TARGET_COLUMN} "
        f"(Top {TOP_HEATMAP_FEATURES} Features)"
    )
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.show()

# %%
