import pandas as pd
import os

def extract_factory_filtered_data(file_path: str, output_path: str = None) -> pd.DataFrame:
    """
    Reads an Excel file, filters rows with IDs like 'O3...' of length 10 and specific mid(6,3) codes,
    and extracts ID (mid(3,3)), Flow (Capacity Multiplier), and Technology (mid(6,3)).

    Args:
        file_path: Path to input Excel file.
        output_path: Optional path to save filtered results. If None, saves next to input file.

    Returns:
        Filtered DataFrame with columns ['ID', 'Flow', 'Technology'].
    """

    # Step 1: Read Excel
    df = pd.read_excel(file_path, sheet_name="Operating Units", index_col=None)

    # Step 2: Validate required columns
    if not {"ID", "Capacity Multiplier"}.issubset(df.columns):
        raise KeyError("Expected columns 'ID' and 'Capacity Multiplier' in the Excel file.")

    # Step 3: Convert to string
    s = df["ID"].astype(str)

    # Step 4: Define valid technology codes
    valid_tech_codes = {"119", "120", "121", "123", "140", "141", "160"}

    # Step 5: Apply filter
    mask = (
        (s.str.len() == 10) &
        (s.str[:2] == "O3") &
        (s.str.slice(5, 8).isin(valid_tech_codes))
    )

    # Step 6: Filter rows
    filtered = df.loc[mask].copy()

    # Step 7: Extract parts
    filtered["Technology"] = s[mask].str.slice(5, 8).astype("Int64")
    filtered["Factory ID"] = pd.to_numeric(s[mask].str.slice(2, 5), errors="coerce").astype("Int64")
    filtered["Flow"] = pd.to_numeric(filtered["Capacity Multiplier"], errors="coerce")

    # Step 8: Keep only necessary columns
    out = filtered[["Factory ID", "Flow", "Technology"]].reset_index(drop=True)

    # Step 9: Save output (if desired)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\factory_results.xlsx")
    out.to_excel(output_path, index=False)
    
    # print(f"✅ Saved filtered data to: {output_path}")

    return out

import pandas as pd

def pivot_by_technology(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts a DataFrame with columns [ID, Flow, Technology]
    into a wide format where each Technology code becomes a column.
    """

    # --- Step 1: Validate input columns ---
    required = {"Factory ID", "Flow", "Technology"}
    if not required.issubset(df.columns):
        raise KeyError(f"Expected columns: {required}")

    # --- Step 2: Ensure numeric values ---
    df = df.copy()
    df["Factory ID"] = pd.to_numeric(df["Factory ID"], errors="coerce").astype("Int64")
    df["Flow"] = pd.to_numeric(df["Flow"], errors="coerce")
    df["Technology"] = pd.to_numeric(df["Technology"], errors="coerce").astype("Int64")

    # --- Step 3: Pivot the data ---
    pivot_df = (
        df.pivot_table(
            index="Factory ID",
            columns="Technology",
            values="Flow",
            aggfunc="sum",
            fill_value=0
        )
        .sort_index(axis=1)
        .reset_index()
    )

    # --- Step 4: Ensure all expected Technology columns exist ---
    expected_tech = [119, 120, 121, 123, 140, 141, 160]
    for t in expected_tech:
        if t not in pivot_df.columns:
            pivot_df[t] = 0
    tech_names = {119: "LTHP",120: "MTHP",121: "HTHP", 123: "EBoiler",140: "BBoiler",141: "BFB",160: "CBoiler"}
    pivot_df = pivot_df.rename(columns=tech_names)

    # Reorder columns: ID + sorted tech columns
    pivot_df = pivot_df[["Factory ID"] + [tech_names[t] for t in expected_tech]]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\factory_pivot.xlsx")
    pivot_df.to_excel(output_path, index=False)

    return pivot_df

def merge_factory_reference(pivot_df: pd.DataFrame, reference_path: str, sheet_name: str = "Factory_updated (5)") -> pd.DataFrame:

    # --- Step 1: Validate input DataFrame ---
    if not any(col in pivot_df.columns for col in ["Factory ID", "ID"]):
        raise KeyError("Expected a column named 'Factory ID' or 'ID' in the pivot table.")

    # --- Step 2: Read reference sheet ---
    ref_df = pd.read_excel(reference_path, sheet_name=sheet_name, header=0, index_col=None, usecols="A:BB")
    required_cols = {"ObjectID", "Industry", "Company name","Region","NZTM_X", "NZTM_Y","North_South" }

    if not required_cols.issubset(ref_df.columns):
        raise KeyError(f"Expected columns {required_cols} in the reference sheet.")

    # --- Step 3: Prepare the key columns ---
    pivot_df = pivot_df.copy()
    ref_df = ref_df.copy()
    key_col = "Factory ID" if "Factory ID" in pivot_df.columns else "ID"

    # Ensure numeric keys for reliable merging
    pivot_df[key_col] = pd.to_numeric(pivot_df[key_col], errors="coerce").astype("Int64")
    ref_df["ObjectID"] = pd.to_numeric(ref_df["ObjectID"], errors="coerce").astype("Int64")

    # --- Step 4: Merge by matching IDs ---
    merged = pd.merge(pivot_df,ref_df[["ObjectID", "Industry", "Company name", "Region","NZTM_X", "NZTM_Y","North_South"]],
        left_on=key_col,right_on="ObjectID",how="left")
    # --- Step 5: Clean up ---
    merged = merged.drop(columns=["ObjectID"])
    merged = merged.rename(columns={"Company name": "Factory Name"})
    
    # --- Step 6: Ensure all expected columns exist ---
    expected_cols = [
        "Factory ID", "Industry", "Factory Name", "Region","NZTM_X", "NZTM_Y","North_South" ,"LTHP", "MTHP", "HTHP", "EBoiler", "BBoiler", "BFB", "CBoiler"]
    # Add missing columns if they don't exist
    for col in expected_cols:
        if col not in merged.columns:
            merged[col] = 0 if col not in ["Industry", "Factory Name", "Region","NZTM_X", "NZTM_Y", "North_South" ] else ""    
    merged = merged[expected_cols]
    
    # --- Step 7: Save output beside script ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\pivot_with_factory_info.xlsx")
    merged.to_excel(output_path, index=False)

    print(f"✅ Saved enriched pivot with factory info to:\n{output_path}")
    # Open the file automatically
    # os.startfile(output_path)
    return merged    




if __name__ == "__main__":
    main_path = r"your main file"
    df_filtered = extract_factory_filtered_data(main_path)
    
    # print(df_filtered.head())

    pivoted = pivot_by_technology(df_filtered)


    reference_path = r"C:\Users\dc278\Downloads\18sept\Compiled Process Heat Data.xlsx"
    
    pd_demand = pd.read_excel(reference_path, sheet_name="Factory_updated (5)", header=0, index_col=None, usecols="A:BB", nrows=429)

    pivot_df_updated_names = merge_factory_reference(pivoted, reference_path, sheet_name="Factory_updated (5)")


