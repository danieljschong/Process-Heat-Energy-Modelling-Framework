import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

# Read Excel file
file_path = r"input file"

df = pd.read_excel(file_path,sheet_name="Sheet1")

# Marker shapes for availability levels
markers = {
    "L0": "P",
    "L1": "o",
    "L2": "^",
    "L3": "s"
}

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

# Plot each availability group with different shape
for availability, marker in markers.items():
    
    subset = df[df["Availability"] == availability]

    ax.scatter(
        subset["Electric"],
        subset["Biomass"],
        subset["R E end Rate"],
        marker=marker,
        s=120,
        edgecolor="black",
        linewidth=0.5,
        label=availability,
        depthshade=False
    )

# Labels
ax.set_xlabel("Electricity Price Change (%)")
ax.set_ylabel("Biomass Price Change (%)")
ax.set_zlabel("Electricity Fuel Switching (%)")


# Only show ticks where data exists
ax.set_xticks(sorted(df["Electric"].unique()))
ax.set_yticks(sorted(df["Biomass"].unique()))
# ax.set_zticks(sorted(df["R E end Rate"].unique()))

ax.legend(title="Availability Level")

output_path = r"output png"

plt.savefig(output_path, dpi=900, bbox_inches="tight",pad_inches=0.3)
plt.show()


