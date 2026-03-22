import os
from pathlib import Path
import math
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Read the exported spreadsheet
script_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(
    script_dir,
    r"Organised_Spreadsheets\biomass_connection_monte_carlo.xlsx"
)

df = pd.read_excel(input_path)

#this box plot shows example of monte carlo on a single site

# Keep only Biomass ID = 2
df = df[df["Biomass ID"] == 2].copy()

# Identify resource flow columns
resource_cols = [
    col for col in df.columns
    if isinstance(col, str) and col.startswith("resource_") and col.endswith("Flow")
]
if not resource_cols:
    raise ValueError("No resource flow columns found.")

# Convert from wide to long format
long_df = df.melt(
    id_vars=["Simulation", "Factory ID", "Biomass ID"],
    value_vars=resource_cols,
    var_name="resource type",
    value_name="Flow"
)

# Extract numeric resource type from names like resource_1Flow
long_df["resource type"] = (
    long_df["resource type"]
    .str.extract(r"resource_(\d+)Flow")[0]
    .astype("Int64")
)

# Optional: drop zero flows so boxplots reflect active values only
long_df = long_df[long_df["Flow"].notna()].copy()
# long_df = long_df[long_df["Flow"] != 0].copy()

# Aggregate in case there are repeated rows
plot_df = (
    long_df.groupby(["Simulation", "Factory ID", "resource type"], as_index=False)["Flow"]
    .sum()
)
# Remove resource type 4 if it is zero everywhere
mask_rt4 = plot_df["resource type"] == 4

if mask_rt4.any():
    if (plot_df.loc[mask_rt4, "Flow"] == 0).all():
        plot_df = plot_df[plot_df["resource type"] != 4]

# Sort for cleaner plotting
plot_df = plot_df.sort_values(["Factory ID", "resource type", "Simulation"]).reset_index(drop=True)

plot_df["Flow"] = plot_df["Flow"] * 1000
factory_flow_max = plot_df.groupby("Factory ID")["Flow"].max() #remove unnecessary low flow factories
valid_factories = factory_flow_max[factory_flow_max >= 1e-6].index
plot_df = plot_df[plot_df["Factory ID"].isin(valid_factories)].copy()


# Get unique factories
factories = sorted(plot_df["Factory ID"].dropna().unique())

if len(factories) == 0:
    raise ValueError("No data found for Biomass ID 2.")

# Create subplot grid
n_factories = len(factories)
ncols = 3
nrows = math.ceil(n_factories / ncols)

fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(5 * ncols, 4 * nrows))
axes = axes.flatten()

resource_labels = {
    0: "In Forest\nHarvest",
    1: "K logs",
    2: "Sawmill\nChips",
    3: "Straw and\nStover",
    4: "Wood\nPellets"
}



for ax, factory_id in zip(axes, factories):
    factory_data = plot_df[plot_df["Factory ID"] == factory_id].copy()
    resource_types = sorted(factory_data["resource type"].dropna().unique())

    box_data = []
    labels = []

    for rt in resource_types:
        vals = factory_data.loc[factory_data["resource type"] == rt, "Flow"].dropna()

        if len(vals) > 0:
            box_data.append(vals.values)
            labels.append(resource_labels.get(int(rt), str(rt)))

    if box_data:
        bp = ax.boxplot(
            box_data,
            labels=labels,
            showmeans=True,
            meanline=True,
            flierprops=dict(marker='o',
        markersize=6,
        markerfacecolor='black',
        markeredgecolor='none',
        alpha=0.25)
        )
        #         ax.set_title(resource_labels.get(n, f"Resource {n}"),fontsize=18)
        # ax.set_ylabel("Flow (GWh)",fontsize=18)
        # ax.set_xlabel("")
        # ax.tick_params(axis='x', labelsize=18)
        # ax.tick_params(axis='y', labelsize=18)
        
        ax.set_title(f"Factory ID = {int(factory_id)}",fontsize=15)
        ax.set_ylabel("Flow (MWh)",fontsize=14)
        ax.set_xlabel("")  # remove x axis title
        ax.tick_params(axis='x', labelsize=13)
        ax.tick_params(axis='y', labelsize=13)

    else:
        ax.set_title(f"Factory {int(factory_id)}")
        ax.set_axis_off()

# Create legend manually
from matplotlib.lines import Line2D
## If there is an used 6th subplot, use it as a legend panel
# legend_elements = [
#     Line2D([0], [0], color="orange", lw=3, label="Median"),
#     Line2D([0], [0], color="green", lw=3, linestyle="--", label="Mean")
# ]

# fig.legend(
#     handles=legend_elements,
#     loc="lower right",
#     ncol=2
# )

# Hide unused axes first
for ax in axes[len(factories):]:
    ax.set_axis_off()
    
from matplotlib.patches import Patch

# If there is an unused 6th subplot, use it as a legend panel
if len(axes) > len(factories):
    legend_ax = axes[len(factories)]
    legend_ax.set_axis_off()

    legend_elements = [
        Patch(facecolor="white", edgecolor="black", label="25th to 75th percentile"),
        Line2D([0], [0], color="black", lw=2.5, label="Non outlier range"),
        Line2D([0], [0], color="orange", lw=3, label="Median"),
        Line2D([0], [0], color="green", lw=3, linestyle="--", label="Mean"),
        Line2D(
            [0], [0],
            marker="o",
            linestyle="None",
            markersize=8,
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
        title="Box plot guide",fontsize=14,title_fontsize=16
    )



# Hide unused axes
for ax in axes[len(factories):]:
    ax.set_axis_off()

plt.tight_layout()

# Save figure
output_plot_path = os.path.join(
    script_dir,
    r"Organised_Spreadsheets\biomass_connection_boxplots_biomass2.png"
)
plt.savefig(output_plot_path, dpi=1000, bbox_inches="tight")
plt.show()

print("Saved plot to:", output_plot_path)


print("\nFactory biomass flow statistics (MWh)")
print("-" * 60)

for factory_id in factories:
    factory_data = plot_df[plot_df["Factory ID"] == factory_id]

    for rt in sorted(factory_data["resource type"].dropna().unique()):
        vals = factory_data.loc[
            factory_data["resource type"] == rt, "Flow"
        ].dropna()

        if len(vals) == 0:
            continue

        q1 = vals.quantile(0.25)
        median = vals.median()
        q3 = vals.quantile(0.75)
        mean = vals.mean()
        iqr = q3 - q1

        print(f"\nFactory {factory_id} — {resource_labels.get(rt, rt)}")
        print(f"Mean   : {mean:.3f}")
        print(f"Median : {median:.3f}")
        print(f"IQR    : {q1:.3f} – {q3:.3f}  (width {iqr:.3f})")
        
summary_rows = []

for factory_id in factories:
    factory_data = plot_df[plot_df["Factory ID"] == factory_id].copy()

    for rt in sorted(factory_data["resource type"].dropna().unique()):
        vals = factory_data.loc[
            factory_data["resource type"] == rt, "Flow"
        ].dropna()

        if len(vals) == 0:
            continue

        q1 = vals.quantile(0.25)
        median = vals.median()
        q3 = vals.quantile(0.75)
        mean = vals.mean()
        iqr = q3 - q1

        summary_rows.append({
            "Factory ID": int(factory_id),
            "Resource Type": int(rt),
            "Resource Label": resource_labels.get(int(rt), str(rt)).replace("\n", " "),
            "Count": len(vals),
            "Mean": mean,
            "Median": median,
            "Q1": q1,
            "Q3": q3,
            "IQR": iqr,
            "Min": vals.min(),
            "Max": vals.max()
        })

summary_df = pd.DataFrame(summary_rows)
output_summary_path = os.path.join(script_dir, r"your output path")
summary_df.to_excel(output_summary_path, index=False)

print("\nSummary table:")
print(summary_df)