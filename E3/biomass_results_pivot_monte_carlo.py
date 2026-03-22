import os
from pathlib import Path
import pandas as pd

base_dir = Path("Select your path")

results = []

#can be done for all or to check for one site.
for num in range(0, 560):
    file_path = base_dir / f"pyomo_results_second_try{num}.xlsx"
    # print(num)
    if not file_path.exists():
        print(f"Missing: {file_path.name}")
        continue

    try:
        # Check Solution sheet first
        solution_df = pd.read_excel(file_path, sheet_name="Solution", header=None)
        value = solution_df.iloc[1, 1]

        # Skip if value is not numeric
        if not pd.notna(pd.to_numeric(value, errors="coerce")):
            continue

        # Read Operating Units sheet
        df = pd.read_excel(file_path, sheet_name="Materials")

        if "ID" not in df.columns:
            raise KeyError("Expected a column named 'ID'")

        s = df["ID"].astype(str)

        mask = (
            (s.str.len() == 7) &
            (s.str[:2] == "M8")
        ) #for all

        # mask = (
        #     (s.str.len() == 7) &
        #     (s.str[:6] == "M80002")
        # ) #only site ID2

        filtered = df.loc[mask].copy()
        s_filtered = filtered["ID"].astype(str)

        if "Lower Bound" in filtered.columns:
            lb_series = filtered["Lower Bound"]
        else:
            raise KeyError("Expected a column named 'Lower Bound'")

        if "Upper Bound" in filtered.columns:
            ub_series = filtered["Upper Bound"]
        else:
            raise KeyError("Expected a column named 'Upper Bound'")

        if "Flow" in filtered.columns:
            flow_series = filtered["Flow"]
        else:
            raise KeyError("Expected a column named 'Flow'")

        out = pd.DataFrame({
            "Simulation": num,
            "Biomass ID": pd.to_numeric(s_filtered.str.slice(2, 6), errors="coerce").astype("Int64"),
            "Lower Bound": pd.to_numeric(lb_series, errors="coerce").astype(float),
            "Upper Bound": pd.to_numeric(ub_series, errors="coerce").astype(float),
            "Flow": pd.to_numeric(flow_series, errors="coerce").astype(float),
            "resource type": pd.to_numeric(s_filtered.str.get(6), errors="coerce").astype("Int64"),
        })

        results.append(out)

        if num % 10 == 0:
            print(num)

    except Exception as e:
        print(f"Error in {file_path.name}: {e}")

if results:
    
    final_df = pd.concat(results, ignore_index=True)
    final_df["High Bound"] = final_df["Lower Bound"].abs()
    final_df["Flow"] = final_df["Flow"].abs()

    # Pivot:
    # index = Simulation, Biomass ID
    # columns = resource type crossed with Flow / Upper Bound / Lower Bound
    pivot_df = final_df.pivot_table(
        index=["Simulation", "Biomass ID"],
        columns="resource type",
        values=["Flow", "High Bound"],
        aggfunc="sum",
        fill_value=0
    )

    # pivot_df = final_df.pivot_table(
    #     index=["Simulation"],
    #     columns="resource type",
    #     values=["Flow", "High Bound"],
    #     aggfunc="sum",
    #     fill_value=0
    # )

    # Flatten multi-level columns into names like:
    # 1_Flow, 1_Upper Bound, 1_Lower Bound
    pivot_df.columns = [
        f"{int(col[1])}_{col[0]}" if pd.notna(col[1]) else f"{col[0]}"
        for col in pivot_df.columns
    ]

    pivot_df = pivot_df.reset_index()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\biomass_monte_carlo.xlsx")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    pivot_df.to_excel(output_path, index=False)
    print("Saved:", output_path)

else:
    print("No valid results found.")