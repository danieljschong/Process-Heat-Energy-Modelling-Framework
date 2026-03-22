import os
import math
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

# Paths
script_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(
    script_dir,
    r"your  input path"
)

name_map = {
    "LTHP": "Low Temperature Heat Pump",
    "MTHP": "Medium Temperature Heat Pump",
    "HTHP": "High Temperature Heat Pump",
    "EBoiler": "Electric Boiler",
    "BBoiler": "Biomass Boiler",
    "BFB": "Bubbling Fluidised Bed",
    "CBoiler": "Coal Retrofitted Boiler",
    "LTStorage": "Low Temperature Storage",
    "MTStorage": "Medium Temperature Storage"
}




# Read data
df = pd.read_excel(input_path)

# Ensure numeric
df["BBoiler"] = pd.to_numeric(df["BBoiler"], errors="coerce")
df["CBoiler"] = pd.to_numeric(df["CBoiler"], errors="coerce")

# Add CBoiler into BBoiler
df["BBoiler"] = df["BBoiler"].fillna(0) + df["CBoiler"].fillna(0)
df = df.drop(columns=["CBoiler"])

df = df.rename(columns=name_map)
df.columns = df.columns.astype(str)

# Columns to plot (exclude Simulation)
tech_cols = [c for c in df.columns if c != "Simulation"]

# Convert to numeric
for c in tech_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")



print("\nTechnology and storage statistics")
print("-" * 60)

stats_rows = []

for col in tech_cols:
    vals = df[col].dropna()

    if vals.empty:
        continue

    q1 = vals.quantile(0.25)
    median = vals.median()
    q3 = vals.quantile(0.75)
    mean = vals.mean()
    iqr = q3 - q1

    print(f"\n{col}")
    print(f"Mean   : {mean:.3f}")
    print(f"Median : {median:.3f}")
    print(f"Q1     : {q1:.3f}")
    print(f"Q3     : {q3:.3f}")
    print(f"IQR    : {iqr:.3f}")

    stats_rows.append({
        "Technology": col,
        "Mean": mean,
        "Median": median,
        "Q1": q1,
        "Q3": q3,
        "IQR": iqr
    })

stats_df = pd.DataFrame(stats_rows)
print("\nSummary table:")
print(stats_df)


# Layout
n_plots = len(tech_cols)
ncols = 2
nrows = math.ceil(n_plots / ncols)

fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(6.5*ncols, 4.8*nrows))
axes = axes.flatten()

for ax, col in zip(axes, tech_cols):

    vals = df[col].dropna()

    if vals.empty:
        ax.set_axis_off()
        continue

    ax.boxplot(
        vals.values,
        showmeans=True,
        meanline=True,
        flierprops=dict(
            marker="o",
            markersize=4,
            markerfacecolor="black",
            markeredgecolor="none",
            alpha=0.25
        )
    )

    ax.set_title(col,fontsize=20)
    ax.set_ylabel("Capacity (MW)", fontsize=18)
    ax.tick_params(axis='y', labelsize=18)
    ax.set_xticks([])

# Hide unused axes
for ax in axes[n_plots:]:
    ax.set_axis_off()

# Use empty subplot as legend panel if available
if len(axes) > n_plots:
    legend_ax = axes[n_plots]
    legend_ax.set_axis_off()

    legend_elements = [
        Patch(facecolor="white", edgecolor="black", label="25th to 75th percentile"),
        Line2D([0], [0], color="black", lw=1.5, label="Non outlier range"),
        Line2D([0], [0], color="orange", lw=2, label="Median"),
        Line2D([0], [0], color="green", lw=2, linestyle="--", label="Mean"),
        Line2D(
            [0], [0],
            marker="o",
            linestyle="None",
            markersize=5,
            markerfacecolor="black",
            markeredgecolor="none",
            alpha=0.25,
            label="Outliers"
        )
    ]

    legend_ax.legend(
        handles=legend_elements,
        loc="center",
        frameon=True,
        title="Box plot guide"
    )
# fig.subplots_adjust(hspace=0.8, top=0.9)
# # plt.tight_layout()
plt.tight_layout(rect=[0, 0, 1, 0.95])

fig.subplots_adjust(
    hspace=0.2,   # vertical spacing between rows
    wspace=0.2,   # horizontal spacing
    top=0.95      # space for title
)

# Save figure
output_plot_path = os.path.join(
    script_dir,
    r"Organised_Spreadsheets\factory_monte_carlo_boxplots.png"
)

plt.savefig(output_plot_path, dpi=300, bbox_inches="tight")
plt.show()

print("Saved plot to:", output_plot_path)