import pandas as pd
import os

def read_excel_from_path(file_path, sheet_name=None, nrows=None):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # print(f"Reading file: {file_path}")
    df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=nrows)
    # print(f"Loaded sheet: {sheet_name or 'first sheet'}, shape: {df.shape}")
    return df


def biomass_material_node(df: pd.DataFrame, flow_col: str = None) -> pd.DataFrame:
    if "ID" not in df.columns:
        raise KeyError("Expected a column named 'ID'")    
    s = df["ID"].astype(str)
    # Filter: IDs like 'O802120'
    mask = (
        (s.str.len() == 7) &      # 7 characters
        (s.str[:2] == "M8")       # starts with O8
    )

    if "Lower Bound" in df.columns:
        lb_series = df["Lower Bound"]
    else:
        raise KeyError("Expected a column named 'Lower Bound'")
    
    if "Upper Bound" in df.columns:
        ub_series = df["Upper Bound"]
    else:
        raise KeyError("Expected a column named 'Upper Bound'")
    
    if "Flow" in df.columns:
        flow_series = df["Flow"]
    
    # Convert to numeric and drop zero / NaN
    lb_series = pd.to_numeric(lb_series, errors="coerce")
    ub_series = pd.to_numeric(ub_series, errors="coerce")
    flow_series = pd.to_numeric(flow_series, errors="coerce")
    
    # Build output DataFrame
    out = pd.DataFrame({
        "Biomass ID": pd.to_numeric(s.str.slice(2, 6), errors="coerce").astype("Int64"),  # Int64 allows NaN
        "Lower Bound": pd.to_numeric(lb_series, errors="coerce").astype(float),
        "Upper Bound": pd.to_numeric(ub_series, errors="coerce").astype(float),      
        "Flow": pd.to_numeric(flow_series, errors="coerce").astype(float),
        "resource type": pd.to_numeric(s.str.get(6), errors="coerce").astype("Int64"),
    })   

    # print(out.head())     
    filtered = out.loc[mask & lb_series.notna() & ub_series.notna() & flow_series.notna()].reset_index(drop=True)  
    return filtered    

def make_biomass_df_op(df: pd.DataFrame, flow_col: str = None) -> pd.DataFrame:
    if "ID" not in df.columns:
        raise KeyError("Expected a column named 'ID'")

    s = df["ID"].astype(str)

    # Filter: IDs like 'O8021203008'
    mask = (
        (s.str.len() == 11) &       # 11 characters
        (s.str[:2] == "O8") &       # starts with O8
        (s.str.get(7) == "3")       # 8th char is '3'
    )

    # Find Capacity Multiplier column (case-insensitive)
    if flow_col is not None:
        cap_series = df.loc[:, flow_col]
    else:
        if "Capacity Multiplier" in df.columns:
            cap_series = df["Capacity Multiplier"]
        else:
            match = [c for c in df.columns if c.lower() == "capacity multiplier"]
            if match:
                cap_series = df[match[0]]
            else:
                # fallback to column E (index 4)
                cap_series = df.iloc[:, 4]

    # Convert to numeric and drop zero / NaN
    cap_series = pd.to_numeric(cap_series, errors="coerce")

    # Apply filters
    valid_capacity = cap_series.notna() & (cap_series != 0)
    combined_mask = mask & valid_capacity

    # Build output DataFrame
    out = pd.DataFrame({
        "Biomass ID": pd.to_numeric(s.str.slice(2, 6), errors="coerce").astype("Int64"),  # Int64 allows NaN
        "Factory ID": pd.to_numeric(s.str.slice(8, 11), errors="coerce").astype("Int64"),
        "Capacity Multiplier": pd.to_numeric(cap_series, errors="coerce").astype(float),
        "resource type": pd.to_numeric(s.str.get(6), errors="coerce").astype("Int64"),
    })


    filtered = out.loc[combined_mask].reset_index(drop=True)
    return filtered

def pivot_capacit_by_resource_mat(df: pd.DataFrame) -> pd.DataFrame:
    # ensure required columns exist
    need = {"Biomass ID", "Lower Bound",  "Flow", "resource type"}
    missing = need - set(df.columns)
    if missing:
        raise KeyError(f"Missing columns: {missing}")
    # make sure resource type is numeric so columns sort correctly
        
    # --- Step 2: Ensure numeric values ---
    df = df.copy()
    df["resource type"] = pd.to_numeric(df["resource type"], errors="coerce")

    df["Upper Bound"] = pd.to_numeric(df["Lower Bound"], errors="coerce").abs()
    df["Flow"] = pd.to_numeric(df["Flow"], errors="coerce").abs()

    # --- Step 3: Aggregate multiple flow rows per Biomass ID ---
    agg_df = (
        df.groupby(["Biomass ID", "resource type"], as_index=False)
          .agg({"Upper Bound": "sum", "Flow": "sum"})
    )

    # --- Step 4: Pivot for Upper Bound ---
    pivot_up = (
        agg_df.pivot_table(
            index="Biomass ID",
            columns="resource type",
            values="Upper Bound",
            aggfunc="sum",
            fill_value=0
        )
        .sort_index(axis=1)
    )
    # Divide all Upper Bound values by 3600
    pivot_up = pivot_up / 3600
    pivot_up.columns = [f"Resource Type {int(c)} Upper Bound" for c in pivot_up.columns]

    # --- Step 5: Pivot for Flow ---
    pivot_flow = (
        agg_df.pivot_table(
            index="Biomass ID",
            columns="resource type",
            values="Flow",
            aggfunc="sum",
            fill_value=0
        )
        .sort_index(axis=1)
    )
    pivot_flow.columns = [f"Flow Resource Type {int(c)}" for c in pivot_flow.columns]

    # --- Step 6: Merge side by side ---
    pivot_combined = pd.concat([pivot_flow, pivot_up], axis=1).reset_index()

    return pivot_combined

def pivot_capacity_by_resource_op(df: pd.DataFrame) -> pd.DataFrame:
    # ensure required columns exist
    need = {"Biomass ID", "Factory ID", "Capacity Multiplier", "resource type"}
    missing = need - set(df.columns)
    if missing:
        raise KeyError(f"Missing columns: {missing}")

    # make sure resource type is numeric so columns sort correctly
    rt = pd.to_numeric(df["resource type"], errors="coerce")
    cm = pd.to_numeric(df["Capacity Multiplier"], errors="coerce")

    tmp = df.copy()
    tmp["resource type"] = rt
    tmp["Capacity Multiplier"] = cm

    # pivot with sum to combine any duplicate rows
    wide = (
        tmp.pivot_table(
            index=["Biomass ID", "Factory ID"],
            columns="resource type",
            values="Capacity Multiplier",
            aggfunc="sum",
            fill_value=0,
        )
        .sort_index(axis=1)   # sort resource columns by type number
        .reset_index()
    )

    # rename resource columns to "Resource Type X"
    new_cols = []
    for c in wide.columns:
        if isinstance(c, (int, float)) or (isinstance(c, str) and c.isdigit()):
            new_cols.append(f"Resource Type {int(c)}")
        else:
            new_cols.append(c)
    wide.columns = new_cols

    return wide

import pandas as pd

def merge_pivot_with_materials(pivot_with_names: pd.DataFrame, pivot_df_m: pd.DataFrame) -> pd.DataFrame:
    """
    Merge pivot_with_names (operations) with pivot_df_m (materials),
    matching on Biomass ID and adding all lower/upper bound columns.
    """
    # --- Step 1: Validate columns ---
    if "Biomass ID" not in pivot_with_names.columns:
        raise KeyError("pivot_with_names must contain 'Biomass ID'")
    if "Biomass ID" not in pivot_df_m.columns:
        raise KeyError("pivot_df_m must contain 'Biomass ID'")

    # --- Step 2: Ensure consistent types ---
    pivot_with_names = pivot_with_names.copy()
    pivot_df_m = pivot_df_m.copy()
    pivot_with_names["Biomass ID"] = pd.to_numeric(pivot_with_names["Biomass ID"], errors="coerce").astype("Int64")
    pivot_df_m["Biomass ID"] = pd.to_numeric(pivot_df_m["Biomass ID"], errors="coerce").astype("Int64")

    # --- Step 3: Select only relevant columns from pivot_df_m ---
    mat_cols = [c for c in pivot_df_m.columns if c.startswith("Resource Type") or c == "Biomass ID"]

    # --- Step 4: Merge on Biomass ID ---
    merged = pivot_with_names.merge(
        pivot_df_m[mat_cols],
        how="left",
        on="Biomass ID"
    )

    # --- Step 5: Return merged DataFrame ---
    return merged



# ------------------------------------------------------------
# Example usage
# ------------------------------------------------------------
if __name__ == "__main__":
    main_path =r"your path here"
    # Step 1: Read Excel
    df_m = read_excel_from_path(main_path, sheet_name="Materials")
    df_op = read_excel_from_path(main_path, sheet_name="Operating Units")

    biomass_df_m = biomass_material_node(df_m)  # your filtered long table  
    pivot_df_m = pivot_capacit_by_resource_mat(biomass_df_m)
    
    biomass_df_op = make_biomass_df_op(df_op)          # your filtered long table
    pivot_df_op   = pivot_capacity_by_resource_op(biomass_df_op)

    # save next to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    



    reference_path = r"C:\Users\dc278\Downloads\18sept\Compiled Process Heat Data.xlsx"
    pd_demand = pd.read_excel(reference_path, sheet_name="Factory_updated (5)", header=0, index_col=None, usecols="A:BB", nrows=429)
    pd_biomass = pd.read_excel(reference_path, sheet_name="biomass_resource", header=0, index_col=None, usecols="A:O",nrows=214)
    pd_biomass_factory_connections = pd.read_excel(reference_path, sheet_name="biomass_factory_distance", header=0, index_col=None,
                                               usecols="A:I", nrows=27564)    

# --- Step 2: Ensure columns are as expected ---
pd_biomass_factory_connections.columns = pd_biomass_factory_connections.columns.str.strip()
pd_biomass_factory_connections["Source ID"] = pd.to_numeric(pd_biomass_factory_connections["Source ID"], errors="coerce").astype("Int64")
pd_biomass_factory_connections["Destination ID"] = pd.to_numeric(pd_biomass_factory_connections["Destination ID"], errors="coerce").astype("Int64")
pd_biomass_factory_connections["Distance"] = pd.to_numeric(pd_biomass_factory_connections["Distance"], errors="coerce")

#Coordinates
pd_biomass_factory_connections["Source Latitude"] = pd.to_numeric(pd_biomass_factory_connections["Source Latitude"], errors="coerce")
pd_biomass_factory_connections["Source Longitude"] = pd.to_numeric(pd_biomass_factory_connections["Source Longitude"], errors="coerce")
pd_biomass_factory_connections["Destination Latitude"] = pd.to_numeric(pd_biomass_factory_connections["Destination Latitude"], errors="coerce")
pd_biomass_factory_connections["Destination Longitude"] = pd.to_numeric(pd_biomass_factory_connections["Destination Longitude"], errors="coerce")


pivot_df_op["Factory ID"] = pd.to_numeric(pivot_df_op["Factory ID"], errors="coerce").astype("Int64")
pivot_df_op["Biomass ID"] = pd.to_numeric(pivot_df_op["Biomass ID"], errors="coerce").astype("Int64")

if not {"ObjectID", "Company name"}.issubset(pd_demand.columns):
    raise KeyError("Expected columns 'ObjectID' and 'Company name' in reference sheet.")

if not {"ORIG_FID", "Row Labels"}.issubset(pd_biomass.columns):
    raise KeyError("Expected columns 'ORIG_FID' and 'Row Labels' in reference sheet.")


# --- Step 3: Prepare the lookup DataFrame ---
factory_lookup = pd_demand[["ObjectID", "Company name"]].copy()
biomass_lookup = pd_biomass[["ORIG_FID", "Row Labels"]].copy()


# --- Step 4: Merge with your pivot_df ---
# Assuming your pivot_df already exists with column 'Factory ID'
pivot_df_op = pivot_df_op.merge(
    factory_lookup, how="left", left_on="Factory ID", right_on="ObjectID")

pivot_df_op = pivot_df_op.drop(columns=["ObjectID"])  # remove duplicate key column
pivot_df_op = pivot_df_op.rename(columns={"Company name": "Factory Name"})  # clearer naming

pivot_df_op = pivot_df_op.merge(biomass_lookup,how="left",left_on="Biomass ID",right_on="ORIG_FID")

pivot_df_op = pivot_df_op.drop(columns=["ORIG_FID"])
pivot_df_op = pivot_df_op.rename(columns={"Row Labels": "Biomass Name"})

# --- Step 5: Merge in distance + coordinates ---
pivot_df_op = pivot_df_op.merge(pd_biomass_factory_connections[
        ["Source ID","Destination ID","Distance",
            "Source Latitude","Source Longitude","Destination Latitude",
            "Destination Longitude",]],
    how="left",
    left_on=["Factory ID", "Biomass ID"],
    right_on=["Source ID", "Destination ID"],)

pivot_df_op = pivot_df_op.drop(columns=["Source ID", "Destination ID"])  # redundant keys

# Rename coordinates: Source -> Factory, Destination -> Biomass
pivot_df_op = pivot_df_op.rename(columns={
    "Source Latitude": "Factory Latitude",
    "Source Longitude": "Factory Longitude",
    "Destination Latitude": "Biomass Latitude",
    "Destination Longitude": "Biomass Longitude",
})

# --- Step 6: Show or save ---
# Optional: save to Excel next to your script
cols = [
    "Biomass ID", "Biomass Name","Biomass Latitude","Biomass Longitude", "Factory ID", "Factory Name","Factory Latitude","Factory Longitude", "Distance"
] + [c for c in pivot_df_op.columns if c.startswith("Resource Type")]
pivot_df_op = pivot_df_op[cols]

# script_dir = os.path.dirname(os.path.abspath(__file__))
# output_path = os.path.join(script_dir, "biomass_pivot_with_names.xlsx")
# pivot_df_op.to_excel(output_path, index=False)
# print(f"✅ Saved merged pivot: {output_path}")    

pivot_final = merge_pivot_with_materials(pivot_df_op, pivot_df_m)
script_dir = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(script_dir, r"Organised_Spreadsheets\biomass_factory_combined.xlsx")
pivot_final.to_excel(output_path, index=False)
# print(f"✅ Saved merged file")



#this is for the biomasssss use
# Columns you want to keep (grouping keys)
key_cols = [
    "Biomass ID", "Biomass Name", "Biomass Latitude", "Biomass Longitude"]

# Detect columns to sum: "Resource Type 0" ... but NOT the "Upper Bound" ones
rt_sum_cols = [
    c for c in pivot_final.columns
    if c.startswith("Resource Type") and "Upper Bound" not in c
]

# Detect columns to average: "Resource Type 0 Upper Bound" ...
rt_avg_cols = [
    c for c in pivot_final.columns
    if c.startswith("Resource Type") and "Upper Bound" in c
]

# Build aggregation map
agg_map = {c: "sum" for c in rt_sum_cols}
agg_map.update({c: "mean" for c in rt_avg_cols})

# Optional: if you also want to keep Distance, decide how to aggregate it (example: min)
# agg_map["Distance"] = "min"
# and include "Distance" in key_cols only if it is identical within each group (usually it is not)

pivot_summary = (
    pivot_final
    .groupby(key_cols, dropna=False, as_index=False)
    .agg(agg_map)
)

# Save
script_dir = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(script_dir, r"Organised_Spreadsheets\biomass_summary.xlsx")
pivot_summary.to_excel(output_path, index=False)
print(f"✅ Saved summary file")