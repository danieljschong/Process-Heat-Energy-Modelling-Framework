import pandas as pd
import os


def extract_poc_connections(file_path: str, sheet_name: str = "Operating Units") -> pd.DataFrame:
    """
    Reads an Excel file and extracts POC connection info based on ID pattern:
      =AND(LEN(ID)=11, RIGHT(ID)="3", LEFT(ID)="O6")

    Builds a table:
      POC ID 1 | POC ID 2 | Lines No | Capacity
      MID(ID,3,3) | MID(ID,7,3) | second-last char of ID | Capacity Multiplier
    """

    # --- Step 1: Read the Excel file ---
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
    if "ID" not in df.columns or "Capacity Multiplier" not in df.columns:
        raise KeyError("Expected columns 'ID' and 'Capacity Multiplier' in sheet.")

    s = df["ID"].astype(str)

    # --- Step 2: Apply the logical mask ---
    mask = (s.str.len() == 12) & s.str.startswith("O6") & s.str.endswith("3")

    filtered = df.loc[mask].copy()
    # print(filtered)
    if filtered.empty:
        print("⚠️ No matching IDs found (LEN=12, LEFT='O6', RIGHT='3').")
        return pd.DataFrame(columns=["POC ID 1", "POC ID 2", "Lines No", "Capacity"])

    # --- Step 3: Extract components ---
    filtered["POC ID 1"] = pd.to_numeric(s[mask].str.slice(2, 5), errors="coerce").astype("Int64")  # MID(ID,3,3)
    filtered["POC ID 2"] = pd.to_numeric(s[mask].str.slice(6, 9), errors="coerce").astype("Int64")  # MID(ID,7,3)
    filtered["Lines No"] = s[mask].str[-3:-1].astype("Int64")  # second-last character
    filtered["Capacity"] = pd.to_numeric(filtered["Capacity Multiplier"], errors="coerce").fillna(0)

    # --- Step 4: Build final dataframe ---
    out = filtered[["POC ID 1", "POC ID 2", "Lines No", "Capacity"]].reset_index(drop=True)

    # --- Step 5: Save to Excel ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\transmission_grid_with_POC.xlsx")
    out.to_excel(output_path, index=False)

    # print(f"✅ Extracted POC connections saved to:\n{output_path}")
    return out



def attach_gxp_metadata_keep_points(df_poc: pd.DataFrame,
                                    ref_path: str,
                                    sheet_name: str = "gxp_gxp_connection_v1",
                                    output_filename: str = r"Organised_Spreadsheets\transmission_grid_with_points.xlsx") -> pd.DataFrame:
    """
    Match POC ID 1 (1-based index) with gxp_gxp_connection_v1 rows,
    keep 'Point 1' and 'Point 2', and append 'Lines No' and 'Capacity'.

    Output columns:
      Point 1 | Point 2 | Grid | Distance | North_South | Value (MVA) | kV | line type 2 | Lines No | Capacity
    """

    # --- Step 1: Validate df_poc ---
    required_cols = {"POC ID 1", "Lines No", "Capacity"}
    if not required_cols.issubset(df_poc.columns):
        raise KeyError(f"Missing columns in df_poc: {required_cols - set(df_poc.columns)}")

    # Fill missing numeric values in df_poc with 0
    df_poc["Lines No"] = pd.to_numeric(df_poc["Lines No"], errors="coerce").fillna(0).astype(int)
    df_poc["Capacity"] = pd.to_numeric(df_poc["Capacity"], errors="coerce").fillna(0)

    # --- Step 2: Read reference sheet ---
    ref = pd.read_excel(ref_path, sheet_name=sheet_name, header=0)
    expected_cols = {"Point 1", "Point 2", "Grid", "Distance", "North_South", "Value (MVA)", "kV", "line type 2"}
    if not expected_cols.issubset(ref.columns):
        raise KeyError(f"Missing columns in reference sheet '{sheet_name}': {expected_cols - set(ref.columns)}")

    # Add 1-based index to match POC ID 1 numbering
    ref = ref.reset_index(drop=False)
    ref["POC ID 1"] = ref["index"] 
    ref = ref.drop(columns=["index"])

    # --- Step 3: Merge df_poc metadata by index (POC ID 1) ---
    merged = ref.merge(df_poc[["POC ID 1", "Lines No", "Capacity"]],
                       on="POC ID 1", how="left")

    # --- Step 4: Fill missing with 0 ---
    merged["Lines No"] = merged["Lines No"].fillna(0).astype(int)
    merged["Capacity"] = merged["Capacity"].fillna(0)

    # --- Step 5: Reorder columns ---
    merged = merged[[
        "Point 1", "Point 2", "Grid", "Distance", "North_South",
        "Value (MVA)", "kV", "line type 2", "Lines No", "Capacity"
    ]]

    # --- Step 6: Save output ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, output_filename)
    merged.to_excel(output_path, index=False)

    # print(f"✅ Transmission grid metadata merged and saved to:\n{output_path}")
    return merged


def clean_poc_metadata(df_with_meta: pd.DataFrame,
                       output_filename: str = r"Organised_Spreadsheets\transmission_grid_cleaned_finalised.xlsx") -> pd.DataFrame:
    """
    Cleans the POC metadata dataframe by:
      - Removing rows where 'Lines No' == 0
      - Dropping 'Capacity' column entirely if all values are 0

    Returns the cleaned dataframe and saves it as an Excel file.
    """

    # --- Step 1: Validate ---
    if "Lines No" not in df_with_meta.columns:
        raise KeyError("Expected a 'Lines No' column in the dataframe.")
    if "Capacity" not in df_with_meta.columns:
        raise KeyError("Expected a 'Capacity' column in the dataframe.")

    # --- Step 2: Drop rows where Lines No == 0 ---
    before_count = len(df_with_meta)
    df_cleaned = df_with_meta[df_with_meta["Lines No"] != 0].copy()
    after_count = len(df_cleaned)
    # print(f"🧹 Removed {before_count - after_count} rows with Lines No = 0")

    # --- Step 3: Drop 'Capacity' column if all values are 0 ---
    if (df_cleaned["Capacity"].fillna(0) == 0).all():
        df_cleaned = df_cleaned.drop(columns=["Capacity"])
        # print("⚙️ Dropped 'Capacity' column (all values were 0)")
    else:
        # Keep nonzero capacities, but clean NaNs
        df_cleaned["Capacity"] = df_cleaned["Capacity"].fillna(0)

    # --- Step 4: Save cleaned version ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, output_filename)
    df_cleaned.to_excel(output_path, index=False)

    print(f"✅ Cleaned POC metadata saved to:\n{output_path}")
    return df_cleaned



# Example direct run
if __name__ == "__main__":
    main_path =r"input file"

    df_poc = extract_poc_connections(main_path)

    ref_path = r"ref file"

    df_with_meta = attach_gxp_metadata_keep_points(df_poc, ref_path)
    # print(df_with_meta.head())
    df_cleaned = clean_poc_metadata(df_with_meta)
    # print(df_cleaned.head())
    
    