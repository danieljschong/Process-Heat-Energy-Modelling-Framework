import pandas as pd
import os

def summarize_generators_by_key(file_path: str) -> None:
    """
    Reads an Excel file and filters IDs into O10, O14, O18 groups:
      - O14: 6 characters long, starts with 'O14'
      - O18: 8 characters long, starts with 'O18'
      - O10: 11 characters long, starts with 'O10' and MID(7,3) == '230'
      - O12: 6 or 8 characters long, starts with 'O12'

    Each group is aggregated (sum of Capacity Multiplier by Gen ID),
    and saved into one Excel file with 3 sheets: O10, O14, O18.
    """

    # --- Step 1: Read Excel ---
    df = pd.read_excel(file_path, sheet_name="Operating Units", index_col=None)

    # --- Step 2: Validate columns ---
    if not {"ID", "Capacity Multiplier"}.issubset(df.columns):
        raise KeyError("Expected columns 'ID' and 'Capacity Multiplier' in input file.")

    s = df["ID"].astype(str)

    # --- Step 3: Define filters ---
    mask_O14 = (s.str.len() == 6) & (s.str[:3] == "O14")
    mask_O12 = (s.str.len() == 6) & (s.str[:3] == "O12") | (s.str.len() == 8) & (s.str[:3] == "O12")
    mask_O18 = (s.str.len() == 8) & (s.str[:3] == "O18")
    mask_O10_230 = s.str.startswith("O10") & (s.str[6:9] == "230") & (s.str.len() == 11)

    # --- Step 4: Extract Gen ID for all rows ---
    df["Gen ID"] = pd.to_numeric(s.str.slice(3, 6), errors="coerce").astype("Int64")

    # --- Step 5: Create filtered groups ---
    df_O14 = df.loc[mask_O14, ["Gen ID", "Capacity Multiplier"]].copy()
    df_O10 = df.loc[mask_O10_230, ["Gen ID", "Capacity Multiplier"]].copy()
    df_O12 = df.loc[mask_O12, ["Gen ID", "Capacity Multiplier"]].copy()
    df_O18 = df.loc[mask_O18, ["Gen ID", "Capacity Multiplier"]].copy()

    # --- Step 6: Aggregate each group by Gen ID ---
    df_O14 = df_O14.groupby("Gen ID", as_index=False)["Capacity Multiplier"].sum()
    df_O10 = df_O10.groupby("Gen ID", as_index=False)["Capacity Multiplier"].sum()
    df_O12 = df_O12.groupby("Gen ID", as_index=False)["Capacity Multiplier"].sum()
    df_O18 = df_O18.groupby("Gen ID", as_index=False)["Capacity Multiplier"].sum()

    # --- Step 7: Save results to Excel ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\generator_capacity_summary.xlsx")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_O14.to_excel(writer, sheet_name="O14", index=False)
        df_O10.to_excel(writer, sheet_name="O10", index=False)
        df_O12.to_excel(writer, sheet_name="O12", index=False)
        df_O18.to_excel(writer, sheet_name="O18", index=False)

    print(f"✅ Saved generator summaries to:\n{output_path}")
    return {"O14":df_O14, "O10": df_O10, "O12": df_O12, "O18": df_O18, "path": output_path}



def generation_reference(ref_path: str, summary_path: str):
    """
    Merges reference generation data with capacity summaries (O10, O14, O18).
    Produces final sheet:
      ObjectID | Types of power station | Type Name | O10 | O14 | O18 | O12
    Missing capacities are filled with 0.
    """

    # --- Step 1: Read reference file ---
    ref_df = pd.read_excel(ref_path, sheet_name="Generation_updated_v1", header=0)
    if not {"ObjectID", "Types of power station", "Type", "Name", "Capacity (MW)", "Status","Region","North_South"}.issubset(ref_df.columns):
        raise KeyError("Expected columns 'ObjectID', 'Types of power station', 'Type', 'Name', 'Capacity (MW)', 'NZTM_X',	'NZTM_Y', 'Region', 'North_South' in reference file.")


    ref_df = ref_df[["ObjectID", "Types of power station", "Type", "Name","Capacity (MW)", "Status",'NZTM_X','NZTM_Y',"Region","North_South"]]

    # --- Step 2: Read summary Excel ---
    o14 = pd.read_excel(summary_path, sheet_name="O14")
    o10 = pd.read_excel(summary_path, sheet_name="O10")
    o12 = pd.read_excel(summary_path, sheet_name="O12")
    o18 = pd.read_excel(summary_path, sheet_name="O18")

    # --- Step 3: Rename capacity columns ---
    o14 = o14.rename(columns={"Capacity Multiplier": "Capacity"})
    o10 = o10.rename(columns={"Capacity Multiplier": "Methanol"})
    o12 = o12.rename(columns={"Capacity Multiplier": "Utilised"})
    o18 = o18.rename(columns={"Capacity Multiplier": "Electricity"})
    o12["Utilised"] = o12["Utilised"].apply(lambda x: 1 if pd.notna(x) and x > 0 else 0)


    # --- Step 4: Merge all ---
    merged = ref_df.merge(o14, left_on="ObjectID", right_on="Gen ID", how="left")
    merged = merged.drop(columns=["Gen ID"], errors="ignore")

    merged = merged.merge(o10, left_on="ObjectID", right_on="Gen ID", how="left")
    merged = merged.drop(columns=["Gen ID"], errors="ignore")

    merged = merged.merge(o12, left_on="ObjectID", right_on="Gen ID", how="left")
    merged = merged.drop(columns=["Gen ID"], errors="ignore")

    merged = merged.merge(o18, left_on="ObjectID", right_on="Gen ID", how="left")
    merged = merged.drop(columns=["Gen ID"], errors="ignore")

    # --- Step 5: Clean up ---
    merged = merged.drop(columns=[col for col in merged.columns if "Gen ID" in col])
    merged = merged.fillna({"Capacity": 0, "Methanol": 0, "Electricity": 0, "Utilised": 0})

    # --- Step 6: Update Capacity when Utilised == 1 and Status == 'Commissioning'
    mask = (merged["Utilised"] == 1) & (merged["Status"] == "Commissioning")
    merged.loc[mask, "Capacity"] = merged.loc[mask, "Capacity (MW)"]
    # --- Step 6: Save output ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\generation_reference_output.xlsx")
    merged.to_excel(output_path, index=False)

    print(f"✅ Merged generation reference saved to:\n{output_path}")
    return merged



# Example direct run
if __name__ == "__main__":
    main_path = r"main path here"
    df_filtered = summarize_generators_by_key(main_path)

    ref_path = r"ref file path here"
    # print("Hello")
    merged = generation_reference(ref_path, df_filtered["path"])

    delete_path_1 = r"C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\monte_carlo_codes\Results presentation\Organised_Spreadsheets\generator_capacity_summary.xlsx"
    if os.path.exists(delete_path_1):
        os.remove(delete_path_1)