import os
import math
import pandas as pd
import matplotlib.pyplot as plt

# Read spreadsheet
script_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(script_dir, r"Organised_Spreadsheets\used_plots\your input path.xlsx")

df = pd.read_excel(input_path)

# Optional: make sure all column names are strings
df.columns = df.columns.astype(str)

# Resource labels
resource_labels = {
    0: "In Forest Harvest",
    1: "K logs",
    2: "Sawmill Chips",
    3: "Straw and Stover",
    4: "Wood Pellets",
}

# Find available resource numbers from columns like 0_Flow, 0_High Bound
resource_ids = []
for col in df.columns:
    if col.endswith("_Flow"):
        try:
            n = int(col.split("_")[0])
            resource_ids.append(n)
        except ValueError:
            pass

resource_ids = sorted(set(resource_ids))

# Keep only resources where at least one of Flow or High Bound is not all zero
resources_to_plot = []
for n in resource_ids:
    flow_col = f"{n}_Flow"
    high_col = f"{n}_High Bound"

    if flow_col not in df.columns or high_col not in df.columns:
        continue

    flow_nonzero = df[flow_col].fillna(0).ne(0).any()
    high_nonzero = df[high_col].fillna(0).ne(0).any()

    if flow_nonzero or high_nonzero:
        resources_to_plot.append(n)

if not resources_to_plot:
    raise ValueError("No nonzero Flow or High Bound columns found to plot.")

# Create subplot grid
n_plots = len(resources_to_plot)
ncols = 2
nrows = math.ceil(n_plots / ncols)

fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(5 * ncols, 4.5 * nrows))
axes = axes.flatten() if hasattr(axes, "__len__") else [axes]

for ax, n in zip(axes, resources_to_plot):
    flow_col = f"{n}_Flow"
    high_col = f"{n}_High Bound"

    series_list = []
    labels = []

    flow_vals = pd.to_numeric(df[flow_col], errors="coerce").dropna()/3600 #GJ to GWh
    high_vals = pd.to_numeric(df[high_col], errors="coerce").dropna()/3600 #GJ to GWh

    # flow_vals = pd.to_numeric(df[flow_col], errors="coerce").dropna()/3.600 #GJ to MWh
    # high_vals = pd.to_numeric(df[high_col], errors="coerce").dropna()/3.600 #GJ to MWh


    # Only plot a series if it is not all zero
    flow_has_data = not flow_vals.empty and (flow_vals != 0).any()
    high_has_data = not high_vals.empty and (high_vals != 0).any()

    # Special rule for resource 4
    if n == 4:
        if flow_has_data:
            series_list.append(flow_vals.values)
            labels.append("Utilised")
    else:
        if flow_has_data:
            series_list.append(flow_vals.values)
            labels.append("Utilised")

        if high_has_data:
            series_list.append(high_vals.values)
            labels.append("Available")

    if series_list:
        ax.boxplot(
            series_list,
            labels=labels,
            showmeans=True,
            meanline=True,
            flierprops=dict(
                marker="o",
                markersize=6,
                markerfacecolor="black",
                markeredgecolor="none",
                alpha=0.25
            )
        )
        ax.set_title(resource_labels.get(n, f"Resource {n}"),fontsize=18)
        ax.set_ylabel("Flow (GWh)",fontsize=18) #GWh
        ax.set_xlabel("")
        ax.tick_params(axis='x', labelsize=18)
        ax.tick_params(axis='y', labelsize=18)
        
    else:
        ax.set_axis_off()

# Hide unused axes
for ax in axes[n_plots:]:
    ax.set_axis_off()
    
    
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
# If there is an unused subplot (6th), use it as legend panel
if len(axes) > len(resources_to_plot):
    legend_ax = axes[len(resources_to_plot)]
    legend_ax.set_axis_off()

    legend_elements = [
        Patch(facecolor="white", edgecolor="black", label="25th to 75th percentile"),
        Line2D([0], [0], color="black", lw=2.5, label="Non outlier range"),
        Line2D([0], [0], color="orange", lw=3, label="Median"),
        Line2D([0], [0], color="green", lw=3, linestyle="--", label="Mean"),
        Line2D([0], [0],
            marker="o",
            linestyle="None",
            markersize=8,
            markerfacecolor="black",
            markeredgecolor="none",
            alpha=0.25,
            label="Outliers",
        )
    ]

    legend_ax.legend(
        handles=legend_elements,
        loc="center",
        frameon=True,
        title="Box plot guide",fontsize=16,title_fontsize=17
    )



plt.tight_layout()

# Save figure
output_plot_path = os.path.join(
    script_dir,
    r"Organised_Spreadsheets\biomass_monte_carlo_boxplots.png"
)
plt.savefig(output_plot_path, dpi=300, bbox_inches="tight")
plt.show()

for n in resources_to_plot:
    flow_col = f"{n}_Flow"
    high_col = f"{n}_High Bound"

    flow_vals = pd.to_numeric(df[flow_col], errors="coerce").dropna() / 3600
    high_vals = pd.to_numeric(df[high_col], errors="coerce").dropna() / 3600

    resource_name = resource_labels.get(n, f"Resource {n}")

    if not flow_vals.empty and (flow_vals != 0).any():
        q1, q3 = flow_vals.quantile([0.25, 0.75])
        print(f"\n{resource_name} - Utilised")
        print(f"Mean:   {flow_vals.mean():.3f}")
        print(f"Median: {flow_vals.median():.3f}")
        print(f"IQR:    {q1:.3f} to {q3:.3f} (width {q3-q1:.3f})")

    if n != 4 and not high_vals.empty and (high_vals != 0).any():
        q1, q3 = high_vals.quantile([0.25, 0.75])
        print(f"\n{resource_name} - Available")
        print(f"Mean:   {high_vals.mean():.3f}")
        print(f"Median: {high_vals.median():.3f}")
        print(f"IQR:    {q1:.3f} to {q3:.3f} (width {q3-q1:.3f})")

print("Saved plot to:", output_plot_path)