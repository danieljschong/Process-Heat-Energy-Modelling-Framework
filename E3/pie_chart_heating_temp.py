from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# =========================
# SETTINGS
# =========================
FILE_PATH=r"input file here"
SHEET_NAME = None


OUTPUT_FOLDER = Path(r"output folder here")

CATEGORY_COL = "Factory Name"

SERIES_COLS = ["60°C2", "90°C3", "140°C4", "180°C5", ">180°C6"]

RENAME_MAP = {
    "60°C2": "<60°C",
    "90°C3": "60-90°C",
    "140°C4": "90-140°C",
    "180°C5": "140-180°C",
    ">180°C6": ">180°C",
}

DEFAULT_COLOURS = {
    "60°C2": "#0070C0",
    "90°C3": "#CAEEFB",
    "140°C4": "#FFFF00",
    "180°C5": "#FFC000",
    ">180°C6": "#FF0000",
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


def make_factory_pies(df: pd.DataFrame):
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    for _, row in df.iterrows():
        factory = str(row[CATEGORY_COL])

        values = []
        labels = []
        colors = []

        for col in SERIES_COLS:
            val = pd.to_numeric(row[col], errors="coerce")
            if pd.notna(val) and val > 0:
                values.append(val)
                labels.append(RENAME_MAP.get(col, col))
                colors.append(DEFAULT_COLOURS.get(col, None))

        if sum(values) == 0:
            continue  # skip empty factories

        plt.rcParams.update({
            "font.size": 13,
            "axes.titlesize": 16,
        })

        fig, ax = plt.subplots(figsize=(7, 7))

        total = sum(values)

        wedges, _ = ax.pie(
            values,
            labels=labels,
            colors=colors,
            startangle=90,
            counterclock=False,
            wedgeprops={"edgecolor": "white", "linewidth": 1},
        )


        # Add value labels with white box
        for wedge, value in zip(wedges, values):

            angle = (wedge.theta2 + wedge.theta1) / 2
            x = 0.65 * np.cos(np.deg2rad(angle))
            y = 0.65 * np.sin(np.deg2rad(angle))

            ax.text(
                x,
                y,
                f"{value:,.1f}",
                ha="center",
                va="center",
                fontsize=12,
                bbox=dict(
                    boxstyle="round,pad=0.3",
                    facecolor="white",
                    edgecolor="none"
                )
            )

        ax.set_title(factory)

        fig.tight_layout()

        safe_name = factory.replace("/", "_").replace("\\", "_")
        output_path = OUTPUT_FOLDER / f"{safe_name}_temperature_pie.png"

        fig.savefig(output_path, dpi=300)
        plt.close(fig)

        print(f"Saved {output_path}")


# =========================
# RUN
# =========================
if __name__ == "__main__":
    df = read_table(Path(FILE_PATH), sheet=SHEET_NAME)
    make_factory_pies(df)