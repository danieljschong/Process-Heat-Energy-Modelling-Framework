from pathlib import Path
import pandas as pd
import os


# Folder containing the Excel files
base_dir = Path(r"your path")

# Valid technology codes
valid_tech_codes = {"134", "135", "136", "138", "142", "143", "162","179", "180"}
expected_tech = [134, 135, 136, 138, 142, 143, 162,179,180]
tech_names = {134: "LTHP",135: "MTHP",136: "HTHP", 138: "EBoiler",142: "BBoiler",143: "BFB",162: "CBoiler",179: "LTStorage",180: "MTStorage"}

# drop_factory_ids_wood = {2,4,8,11,12,17,18,23,27,29,33,34,36,40,41,45,49,55,57,60,62,65,67,69,70,75,
# 76,80,83,85,87,89,90,92,98,100,101,102,103,105,106,107,108,109,110,119,120,
# 121,123,124,125,127,131,133,136,143,146,147,148,149,150,157,161,166,176,179,
# 189,195,199,221,222,287,348,351,364}
drop_factory_ids_wood={}
results = []

#simulation runs
for num in range(0, 560):
    if num % 5 == 0:
        print(num)
    file_path = base_dir / f"pyomo_results_second_try{num}.xlsx"

    if not file_path.exists():
        print(f"Missing: {file_path.name}")
        continue

    try:
        solution_df = pd.read_excel(file_path, sheet_name="Solution", header=None)
        value = solution_df.iloc[1, 1]

        if pd.isna(value) or str(value).strip() == "":
            continue

        df = pd.read_excel(file_path, sheet_name="Operating Units")

        s = df["ID"].astype(str)

        mask = (
            (s.str.len() == 8) &
            (s.str[:2] == "O3") &
            (s.str.slice(5, 8).isin(valid_tech_codes))
        )

        filtered = df.loc[mask].copy()

        filtered["Technology"] = s[mask].str.slice(5, 8).astype("Int64")
        filtered["Factory ID"] = pd.to_numeric(
            s[mask].str.slice(2, 5), errors="coerce"
        ).astype("Int64")

        filtered["Flow"] = pd.to_numeric(
            filtered["Capacity Multiplier"], errors="coerce"
        )

        filtered = filtered[~filtered["Factory ID"].isin(drop_factory_ids_wood)].copy()

        
        out = filtered[["Factory ID", "Flow", "Technology"]].reset_index(drop=True)

        # keep track of simulation number
        out["Simulation"] = num

        results.append(out)

    except Exception as e:
        print(f"Error in {file_path.name}: {e}")

#combine everything
if results:
    final_df = pd.concat(results, ignore_index=True)

    final_df_9 = final_df[final_df["Factory ID"] == 9].copy()
    
    pivot_df_9 = final_df_9.pivot_table(
        index=["Simulation"],
        columns="Technology",
        values="Flow",
        aggfunc="sum",
        fill_value=0
    )
    
    pivot_df = final_df.pivot_table(
    index="Simulation",
    columns="Technology",
    values="Flow",
    aggfunc="sum",
    fill_value=0)

    pivot_df = pivot_df.reindex(columns=expected_tech, fill_value=0)
    pivot_df_9 = pivot_df_9.reindex(columns=expected_tech, fill_value=0)
    # Replace technology codes with names
    pivot_df = pivot_df.rename(columns=tech_names)
    pivot_df_9 = pivot_df_9.rename(columns=tech_names)

    pivot_df = pivot_df.reset_index()
    pivot_df.columns.name = None
    pivot_df_9 = pivot_df_9.reset_index()
    pivot_df_9.columns.name = None
  

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\factory_monte_carlo_storage.xlsx")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    pivot_df.to_excel(output_path, index=False)
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\factory_monte_carlo_storage_one_site.xlsx")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)    
    
    pivot_df_9.to_excel(output_path, index=False)

    print("Saved:", output_path)

else:
    print("No valid results found.")