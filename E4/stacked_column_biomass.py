from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch


# =========================
# SETTINGS (edit these)
# =========================
SHEET_NAME = None
FILE_PATH = r"file input here"
OUTPUT_PNG = r"output png"

CATEGORY_COL = "Biomass Name"  # <-- FIXED (this is your region label)

SERIES_COLS_BASE = [
    "Resource Type 0", "Resource Type 1", "Resource Type 2", "Resource Type 3", "Resource Type 4"
]

SERIES_COLS_UB = [
    "Resource Type 0 Upper Bound", "Resource Type 1 Upper Bound", "Resource Type 2 Upper Bound",
    "Resource Type 3 Upper Bound", "Resource Type 4 Upper Bound"
]

TITLE = "Biomass resources by region (base vs available limit)"
Y_LABEL = "Biomass Resource (GWh/a)"

# Pretty names for legend only
LABEL_MAP = {
    "Resource Type 0": "In forest harvest",
    "Resource Type 1": "K logs",
    "Resource Type 2": "Sawmill chip",
    "Resource Type 3": "Straw and stover",
    "Resource Type 4": "Wood pellets",
}

DEFAULT_COLOURS = {
    "Resource Type 0": "#4F81BD",
    "Resource Type 1": "#FFC000",
    "Resource Type 2": "#FF0000",
    "Resource Type 3": "#8064A2",
    "Resource Type 4": "#4F6228",
}


# =========================
# FUNCTIONS
# =========================
def read_table(path: Path, sheet: str | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()

    if suffix in [".xlsx", ".xlsm", ".xls"]:
        if sheet is None:
            return pd.read_excel(path)
        return pd.read_excel(path, sheet_name=sheet)

    if suffix == ".csv":
        return pd.read_csv(path)

    raise ValueError("Supported inputs are .xlsx .xls .xlsm .csv")

def make_stacked_by_region(
    df: pd.DataFrame,
    category_col: str,
    base_cols: list[str],
    colours: dict[str, str],
    label_map: dict[str, str],
    title: str,
    y_label: str,
    output_png: Path,
):
    needed = [category_col, *base_cols]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in data: {missing}")

    plot_df = df[[category_col, *base_cols]].copy()

    for c in base_cols:
        plot_df[c] = pd.to_numeric(plot_df[c], errors="coerce").fillna(0.0)

    # Aggregate by region
    grouped = plot_df.groupby(category_col)[base_cols].sum()

    # Sort by total stack descending
    totals = grouped.sum(axis=1)
    grouped = grouped.loc[totals.sort_values(ascending=False).index]

    regions = grouped.index.astype(str).tolist()
    n = len(regions)

    x = np.arange(n)

    plt.rcParams.update({
        "font.size": 14,
        "axes.titlesize": 18,
        "axes.labelsize": 16,
        "xtick.labelsize": 18,
        "ytick.labelsize": 18,
        "legend.fontsize": 14,
    })

    fig, ax = plt.subplots(figsize=(16, 8))

    bottom = np.zeros(n)

    for col in base_cols:
        values = grouped[col].values
        colour = colours.get(col, None)
        label = label_map.get(col, col)

        ax.bar(
            x,
            values,
            bottom=bottom,
            label=label,
            color=colour,
        )

        bottom = bottom + values

    ax.set_title(title, pad=12)
    ax.set_ylabel(y_label)
    ax.set_ylim(0, 4000)
    ax.set_xticks(x)
    ax.set_xticklabels(regions, rotation=45, ha="right")

    ax.legend(title="Resource type", frameon=False)

    fig.tight_layout()
    fig.savefig(output_png, dpi=300)
    plt.close(fig)
    
def make_two_stacks_side_by_side(
    df: pd.DataFrame,
    category_col: str,
    base_cols: list[str],
    ub_cols: list[str],
    colours: dict[str, str],
    label_map: dict[str, str],
    title: str,
    y_label: str,
    output_png: Path,
):
    needed = [category_col, *base_cols, *ub_cols]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in data: {missing}")

    plot_df = df[[category_col, *base_cols, *ub_cols]].copy()

    for c in base_cols + ub_cols:
        plot_df[c] = pd.to_numeric(plot_df[c], errors="coerce").fillna(0.0)

    grouped = plot_df.groupby(category_col)[base_cols + ub_cols].sum()
    
    # grouped = plot_df.groupby(category_col)[base_cols + ub_cols].sum()
    grouped = grouped.loc[grouped.sum(axis=1).sort_values(ascending=False).index]
    
    
    grouped = plot_df.groupby(category_col)[base_cols + ub_cols].sum()

    # Sort by total upper bound stack
    upper_totals = grouped[ub_cols].sum(axis=1)
    grouped = grouped.loc[upper_totals.sort_values(ascending=False).index]

    regions = grouped.index.astype(str).tolist()
    n = len(regions)

    x = np.arange(n)
    width = 0.35
    x_base = x - width / 2
    x_ub = x + width / 2

    plt.rcParams.update({
        "font.size": 14,
        "axes.titlesize": 18,
        "axes.labelsize": 16,
        "xtick.labelsize": 18,
        "ytick.labelsize": 18,
        "legend.fontsize": 14,
    })

    fig, ax = plt.subplots(figsize=(16, 8))

    bottom_base = np.zeros(n)
    bottom_ub = np.zeros(n)

    # Draw stacks: left = base, right = upper bound
    for base_col, ub_col in zip(base_cols, ub_cols):
        base_vals = grouped[base_col].values
        ub_vals = grouped[ub_col].values

        col = colours.get(base_col, None)
        label = label_map.get(base_col, base_col)

        ax.bar(
            x_base, base_vals, width=width, bottom=bottom_base,
            label=label, color=col
        )
        ax.bar(
            x_ub, ub_vals, width=width, bottom=bottom_ub,
            color=col, hatch="//", edgecolor="black", linewidth=0.6
        )

        bottom_base = bottom_base + base_vals
        bottom_ub = bottom_ub + ub_vals

    ax.set_title(title, pad=12)
    ax.set_ylabel(y_label)
    # max_height = bottom.max()
    # ax.set_ylim(0, max_height * 1.05)  # 5% space above the tallest bar
    ax.set_ylim(0, 6000)
    ax.set_xticks(x)
    ax.set_xticklabels(regions, rotation=45, ha="right")

    tech_legend = ax.legend(title="Resource type", loc="upper right", frameon=False)
    ax.add_artist(tech_legend)

    base_proxy = Patch(facecolor="white", edgecolor="black", linewidth=0.8, label="Base")
    ub_proxy = Patch(facecolor="white", edgecolor="black", linewidth=0.8, hatch="//", label="Upper bound")
    ax.legend(handles=[base_proxy, ub_proxy], loc="upper right",     bbox_to_anchor=(0.78, 1),frameon=False)

    fig.tight_layout()
    fig.savefig(output_png, dpi=300)
    plt.close(fig)


# =========================
# RUN
# =========================
if __name__ == "__main__":
    df = read_table(Path(FILE_PATH), sheet=SHEET_NAME)

    make_two_stacks_side_by_side(
        df=df,
        category_col=CATEGORY_COL,
        base_cols=SERIES_COLS_BASE,
        ub_cols=SERIES_COLS_UB,
        colours=DEFAULT_COLOURS,
        label_map=LABEL_MAP,
        title=TITLE,
        y_label=Y_LABEL,
        output_png=Path(OUTPUT_PNG),
    )

    # make_stacked_by_region(
    # df=df,
    # category_col=CATEGORY_COL,
    # base_cols=SERIES_COLS_BASE,
    # colours=DEFAULT_COLOURS,
    # label_map=LABEL_MAP,
    # title=TITLE,
    # y_label=Y_LABEL,
    # output_png=Path(OUTPUT_PNG),
    # )

    print(f"Saved {OUTPUT_PNG}")