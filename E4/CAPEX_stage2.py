import pandas as pd
import numpy as np

main_oath = "your pyomo results path"

def get_layer(id_str):
    if pd.isna(id_str):
        return None

    id_str = str(id_str).strip()
    length = len(id_str)

    # proportional cost related
    if id_str.startswith("O14") and length == 6:
        return "Generation"

    if id_str.startswith("O8") and length == 15:
        return "Biomass"

    # fix cost related
    if id_str.startswith("O6") and length == 12 and len(id_str) > 5 and id_str[5] == "6":
        return "Transmission"

    if id_str.startswith("O18") and length == 9 and id_str.endswith("01"): #subtransmission capacity  O18017001
        return "SubTransmission"     

    if id_str.startswith("O3") and length == 10 and id_str[5:8] == "103":
        return "Distribution"

    if id_str.startswith("O2") and length == 8 and id_str.endswith("53"):
        try:
            mid = int(id_str[2:5])
        except ValueError:
            return None

        if 0 <= mid <= 299:
            return "Supply Transformer"
        if 300 <= mid <= 600:
            return "Demand Transformer"

    return None


# read sheet
df_stage_2_Op = pd.read_excel(main_path, sheet_name="Operating Units")

# normalise columns
df_stage_2_Op.columns = (
    df_stage_2_Op.columns.astype(str)
    .str.strip().str.lower()
    .str.replace(" ", "_", regex=False)
)

ID_COL = "id"

required = [ID_COL, "fix_cost", "proportional_cost", "capacity_multiplier"]
missing = [c for c in required if c not in df_stage_2_Op.columns]
if missing:
    raise KeyError(f"Missing required columns: {missing}. Available: {list(df_stage_2_Op.columns)}")

# create Layer column
df_stage_2_Op["Layer"] = df_stage_2_Op["id"].apply(get_layer)

# filter only matched rows
filtered_df_2_Op = df_stage_2_Op[df_stage_2_Op["Layer"].notna()].copy()

# ensure numeric
for c in ["fix_cost", "proportional_cost", "capacity_multiplier"]:
    filtered_df_2_Op[c] = pd.to_numeric(filtered_df_2_Op[c], errors="coerce").fillna(0.0)

# set proportional_cost to 0 for O3...103 group
id_series = filtered_df_2_Op[ID_COL].astype(str).str.strip()
mask_o3_103 = (
    id_series.str.startswith("O3")
    & (id_series.str.len() == 10)
    & (id_series.str[5:8] == "103")
)
filtered_df_2_Op.loc[mask_o3_103, "proportional_cost"] = 0.0

# recompute cost (your rule)
# ensure numeric
for c in ["fix_cost", "proportional_cost", "capacity_multiplier"]:
    filtered_df_2_Op[c] = pd.to_numeric(filtered_df_2_Op[c], errors="coerce").fillna(0.0)

filtered_df_2_Op["cost"] = (
    np.where(
        filtered_df_2_Op["capacity_multiplier"] > 0,
        filtered_df_2_Op["fix_cost"],
        0.0
    )
    +
    filtered_df_2_Op["capacity_multiplier"] * filtered_df_2_Op["proportional_cost"]
)

# now (optionally) drop zero-cost rows based on the recomputed cost
filtered_df_2_Op = filtered_df_2_Op.loc[filtered_df_2_Op["cost"] != 0].copy()

# total for summary sheet
operating_cost_total = filtered_df_2_Op["cost"].sum()


# Aggregate by Layer (costing)
layer_summary = (filtered_df_2_Op
    .groupby("Layer", as_index=False)
    .agg(fix_cost=("fix_cost", "sum"),
        proportional_cost=("proportional_cost", "sum"),
        capacity_multiplier=("capacity_multiplier", "sum"),
        cost=("cost", "sum")))


# compute grand total (before adding total row)
grand_total = layer_summary["cost"].sum()

# add percentage column
layer_summary["percentage_of_total"] = (layer_summary["cost"] / grand_total * 100)

# create total row
total_row = pd.DataFrame([{"Layer": "Total",
    "fix_cost": layer_summary["fix_cost"].sum(),
    "proportional_cost": layer_summary["proportional_cost"].sum(),
    "capacity_multiplier": layer_summary["capacity_multiplier"].sum(),
    "cost": grand_total,
    "percentage_of_total": 100.0}])

# append
layer_summary = pd.concat([layer_summary, total_row], ignore_index=True)


# output
output_path = r"your output path"


with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    filtered_df_2_Op.to_excel(writer, sheet_name="Filtered Operating Units", index=False)
    layer_summary.to_excel(writer, sheet_name="Layer Summary", index=False)

print("Excel file successfully created at:")
print(output_path)

# import matplotlib.pyplot as plt
# import squarify

# plot_df = layer_summary[layer_summary["Layer"] != "Total"].copy()
# plot_df = plot_df[plot_df["cost"] > 0]

# plt.figure(figsize=(7, 7))  # square treemap
# squarify.plot(
#     sizes=plot_df["cost"],
#     label=[f"{l}\n{c/sum(plot_df['cost'])*100:.1f}%" for l, c in zip(plot_df["Layer"], plot_df["cost"])],
#     alpha=0.9
# )
# plt.title("Cost Treemap by Layer")
# plt.axis("off")
# plt.show()