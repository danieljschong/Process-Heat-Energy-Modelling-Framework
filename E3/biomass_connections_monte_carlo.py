import os
from pathlib import Path
import pandas as pd

#this monte carlo shows all the sites

def make_biomass_df_op(df: pd.DataFrame, flow_col: str = None) -> pd.DataFrame:
    if "ID" not in df.columns:
        raise KeyError("Expected a column named 'ID'")

    s = df["ID"].astype(str)

    # mask = (
    #     (s.str.len() == 11) &
    #     (s.str[:2] == "O8") &
    #     (s.str.get(7) == "3")
    # ) #all sites

    mask = (
        (s.str.len() == 11) &
        (s.str[:6] == "O80002") &
        (s.str.get(7) == "3")
    ) #only site id 2



    if flow_col is not None:
        cap_series = df[flow_col]
    else:
        if "Capacity Multiplier" in df.columns:
            cap_series = df["Capacity Multiplier"]
        else:
            match = [c for c in df.columns if c.lower() == "capacity multiplier"]
            if match:
                cap_series = df[match[0]]
            else:
                cap_series = df.iloc[:, 4]

    cap_series = pd.to_numeric(cap_series, errors="coerce")

    valid_capacity = cap_series.notna() & (cap_series != 0)
    combined_mask = mask & valid_capacity

    out = pd.DataFrame({
        "Biomass ID": pd.to_numeric(s.str.slice(2, 6), errors="coerce").astype("Int64"),
        "Factory ID": pd.to_numeric(s.str.slice(8, 11), errors="coerce").astype("Int64"),
        "Flow": cap_series.astype(float),
        "resource type": pd.to_numeric(s.str.get(6), errors="coerce").astype("Int64"),
    })

    filtered = out.loc[combined_mask].reset_index(drop=True)
    return filtered


base_dir = Path("Select your path")

results = []

for num in range(0, 593):
    file_path = base_dir / f"pyomo_results_second_try{num}.xlsx"

    if not file_path.exists():
        print(f"Missing: {file_path.name}")
        continue

    try:
        solution_df = pd.read_excel(file_path, sheet_name="Solution", header=None)
        value = solution_df.iloc[1, 1]

        if not pd.notna(pd.to_numeric(value, errors="coerce")):
            continue

        df = pd.read_excel(file_path, sheet_name="Operating Units")

        out = make_biomass_df_op(df)
        out["Simulation"] = num

        results.append(out)

        if num % 10 == 0:
            print(num)

    except Exception as e:
        print(f"Error in {file_path.name}: {e}")

if results:
    final_df = pd.concat(results, ignore_index=True)

    # Pivot table
    pivot_df = final_df.pivot_table(
        index=["Simulation", "Biomass ID", "Factory ID"],
        columns="resource type",
        values="Flow",
        aggfunc="sum",
        fill_value=0
    )

    # Flatten column names
    pivot_df.columns = [f"resource_{int(c)}Flow" for c in pivot_df.columns]

    pivot_df = pivot_df.reset_index()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(
        script_dir,
        r"Organised_Spreadsheets\biomass_connection_monte_carlo.xlsx"
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    pivot_df.to_excel(output_path, index=False)

    print("Saved:", output_path)

else:
    print("No valid results found.")
