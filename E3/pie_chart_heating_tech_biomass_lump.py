from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# =========================
# SETTINGS
# =========================
FILE_PATH = r"input excel file"


SHEET_NAME = None


OUTPUT_FOLDER = Path(r"output path")


CATEGORY_COL = "Factory Name"

SERIES_COLS = ["LTHP", "MTHP", "HTHP", "EBoiler", "BBoiler", "BFB", "CBoiler"]

RENAME_MAP = {
    "BBoiler": "BB",
    # add more if needed, eg:
    # "Eboiler": "EBoiler",
}

DEFAULT_COLOURS = {
    "LTHP": "#4F81BD",
    "MTHP": "#FFC000",
    "HTHP": "#FF0000",
    "EBoiler": "#8064A2",
    "BBoiler": "#4F6228",
    "BFB":"#00B050" ,
    "CBoiler": "#000000",
}

# Combine these into one category
BIOMASS_GROUP = ["BBoiler", "BFB", "CBoiler"]
BIOMASS_LABEL = "Biomass-based Boiler"
BIOMASS_COLOR = "#0B6623"


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


def make_factory_pies(df: pd.DataFrame):
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    for _, row in df.iterrows():
        factory = str(row[CATEGORY_COL])

        values = []
        labels = []
        colors = []

        # Sum biomass-based boiler columns
        biomass_total = 0
        for col in BIOMASS_GROUP:
            val = pd.to_numeric(row.get(col), errors="coerce")
            if pd.notna(val):
                biomass_total += val    

        # Add non-biomass technologies
        for col in SERIES_COLS:
            if col in BIOMASS_GROUP:
                continue

            val = pd.to_numeric(row.get(col), errors="coerce")
            if pd.notna(val) and val > 0:
                values.append(val)
                labels.append(RENAME_MAP.get(col, col))
                colors.append(DEFAULT_COLOURS.get(col, None))

        # Add combined biomass slice
        if biomass_total > 0:
            values.append(biomass_total)
            labels.append(BIOMASS_LABEL)
            colors.append(BIOMASS_COLOR)

        if sum(values) == 0:
            continue

        plt.rcParams.update({
            "font.size": 13,
            "axes.titlesize": 16,
        })

        fig, ax = plt.subplots(figsize=(7, 7))
        total = sum(values)

        # Hide tiny slice labels
        display_labels = []
        for v, lab in zip(values, labels):

            # Hide biomass label so we place it manually
            if lab == BIOMASS_LABEL:
                display_labels.append("")
                continue

            if v / total >= 0.01:
                display_labels.append(lab)
            else:
                display_labels.append("")

        wedges, _ = ax.pie(
            values,
            labels=display_labels,
            colors=colors,
            startangle=90,
            counterclock=False,
            wedgeprops={"edgecolor": "white", "linewidth": 1},
        )

        # --- Add biomass label inside slice ---
        for wedge, lab, value in zip(wedges, labels, values):

            if lab != BIOMASS_LABEL:
                continue

            angle = (wedge.theta2 + wedge.theta1) / 2

            r = 0.45  # inside pie

            x = r * np.cos(np.deg2rad(angle))
            y = r * np.sin(np.deg2rad(angle))

            ax.text(
                x,
                y,
                BIOMASS_LABEL,
                ha="center",
                va="center",
                fontsize=12,
                bbox=dict(
                    boxstyle="round,pad=0.35",
                    facecolor="white",
                    edgecolor="none",
                ),
            )

        ax.set_title(factory)
        fig.tight_layout()

        safe_name = factory.replace("/", "_").replace("\\", "_")
        output_path = OUTPUT_FOLDER / f"{safe_name}_heat_tech_pie.png"

        fig.savefig(output_path, dpi=300)
        plt.close(fig)

        print(f"Saved {output_path}")


if __name__ == "__main__":
    df = read_table(Path(FILE_PATH), sheet=SHEET_NAME)
    make_factory_pies(df)
