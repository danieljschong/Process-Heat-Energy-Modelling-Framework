import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


# =========================
# SETTINGS (edit these)
# =========================
FILE_PATH = r"input excel file"
SHEET_NAME = "Sheet2"  # e.g. "Sheet1" or None for first sheet
OUTPUT_PNG = r"output png"

CATEGORY_COL = "Industry"

SERIES_COLS = ["<60C", "60-90C", "90-140C", "140-180C", ">180C"]

RENAME_MAP = {
    "<60C": "<60°C",
    "60-90C": "60-90°C",
    "90-140C": "90-140°C",
    "140-180C": "140-180°C",
    ">180C": ">180°C",
}

TITLE = "Heat supply by technology"

DEFAULT_COLOURS = {
    "<60C": "#0070C0",
    "60-90C": "#00B050",
    "90-140C": "#FFFF00",
    "140-180C": "#FFC000",
    ">180C": "#FF0000",
}

y_label = "Process Heat Demand (GWh)"


# =========================
# FUNCTIONS
# =========================
def read_table(path: Path, sheet: str | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()

    if suffix in [".xlsx", ".xlsm", ".xls"]:
        if sheet is None:
            return pd.read_excel(path)  # first sheet only
        else:
            return pd.read_excel(path, sheet_name=sheet)

    if suffix == ".csv":
        return pd.read_csv(path)

    raise ValueError("Supported inputs are .xlsx .xls .xlsm .csv")

def make_stacked_column(
    df: pd.DataFrame,
    category_col: str,
    series_cols: list[str],
    colours: dict[str, str],
    title: str,
    output_png: Path,
):
    missing = [c for c in [category_col, *series_cols] if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in data: {missing}")

    plot_df = df[[category_col, *series_cols]].copy()

    for c in series_cols:
        plot_df[c] = pd.to_numeric(plot_df[c], errors="coerce").fillna(0.0)

    plot_df = plot_df.set_index(category_col)

    # -----------------------------
    # THESIS FONT SETTINGS
    # -----------------------------
    plt.rcParams.update({
        "font.size": 14,          # base size
        "axes.titlesize": 18,     # title
        "axes.labelsize": 16,     # axis labels
        "xtick.labelsize": 20,
        "ytick.labelsize": 20,
        "legend.fontsize": 14,
    })


    fig, ax = plt.subplots(figsize=(14, 7))

    bottom = None
    x = range(len(plot_df.index))

    for col in series_cols:
        values = plot_df[col].values
        ax.bar(
            x,
            values,
            bottom=bottom,
            label=RENAME_MAP.get(col, col),
            color = DEFAULT_COLOURS.get(col, None)
        )
        bottom = values if bottom is None else (bottom + values)

    # ax.set_title(title)
    # ax.set_xlabel(category_col)
    ax.set_ylabel(y_label)
    ax.set_xticks(list(x))
    ax.set_xticklabels(plot_df.index.astype(str), rotation=60, ha="right")
    ax.legend()

    fig.tight_layout()
    fig.savefig(output_png, dpi=200)
    plt.close(fig)


# =========================
# RUN
# =========================
if __name__ == "__main__":
    df = read_table(Path(FILE_PATH), sheet=SHEET_NAME)

    # if RENAME_MAP:
    #     df = df.rename(columns=RENAME_MAP)
    # Pivot / aggregate by Industry
    # print(df)
    df_grouped = (
        df.groupby(CATEGORY_COL)[SERIES_COLS]
        .sum()
        .reset_index()
    )    

    make_stacked_column(
        df=df_grouped,
        category_col=CATEGORY_COL,
        series_cols=SERIES_COLS,
        colours=DEFAULT_COLOURS,
        title=TITLE,
        output_png=OUTPUT_PNG,
    )

    print(f"Saved {OUTPUT_PNG}")
    
    

