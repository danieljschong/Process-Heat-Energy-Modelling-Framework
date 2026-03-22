import pandas as pd
import os



def subtransmission_line(main_path):
    """
    Reads `main_path` (Excel), sheet `sheet_name`, filters rows where:
      - LEFT(ID,3) == 'O18'
      - LEN(ID) == 9
    Keeps columns: ID, Capacity Multiplier, Lower Bound, Upper Bound
    Adds:
      - Gen ID  = MID(ID,4,3)  (Excel-style; 1-based)
      - Tech    = MID(ID,7,1)
    Exports to a new spreadsheet and returns the filtered DataFrame and output path.
    """
    # Read
    df = pd.read_excel(main_path, sheet_name="Operating Units", engine="openpyxl")

    # Ensure ID as clean string
    id_str = df["ID"].astype(str).str.strip()

    # Filter: LEFT(ID,3)=='O18' and LEN(ID)==9
    mask = (id_str.str[:3] == "O18") & (id_str.str.len() == 9)
    out = df.loc[mask].copy()

    # Create derived columns (Excel MID(ID,4,3) -> Python [3:6]; MID(ID,7,1) -> [6:7])
    out["Gen ID"] = id_str.loc[mask].str[3:6].astype(int)
    out["Tech"]   = id_str.loc[mask].str[6:7].astype(int)

    # Keep requested columns (plus ID)
    cols = [ "Gen ID", "Capacity Multiplier", "Lower Bound", "Upper Bound", "Tech"]
    # Only select those that actually exist to avoid KeyErrors if casing differs, etc.
    missing = [c for c in cols if c not in out.columns]
    if missing:
        raise KeyError(f"Missing expected columns in input: {missing}")

    out = out[cols]

    # Write
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\subtransmission_upgrades.xlsx")
    out.to_excel(output_path, index=False)
    

    return out


def subtransmission_line_names(df_result, ref_path):
    """
    Reads `ref_path` (Excel), sheet 'Generation Updated'.
    Matches df_result['Gen ID'] with ref['ObjectID'].
    Returns a new DataFrame containing:
        Gen ID, Capacity Multiplier, Lower Bound, Upper Bound, Tech,
        Name, Region, North_South
    """
    # Read reference data
    ref_df = pd.read_excel(ref_path, sheet_name="Generation_updated_v1", engine="openpyxl")

    # Ensure comparable types
    df_result["Gen ID"] = df_result["Gen ID"].astype(int)
    ref_df["ObjectID"] = ref_df["ObjectID"].astype(int)

    # Merge on Gen ID ↔ ObjectID
    merged = pd.merge(df_result, ref_df[["ObjectID", "Name", "NZTM_X","NZTM_Y","POC code","GXP NZTM_X","GXP NZTM_Y", "Region", "North_South","Status"]],
                      left_on="Gen ID", right_on="ObjectID", how="left")

    # Drop duplicate ObjectID column (keep Gen ID)
    merged.drop(columns=["ObjectID"], inplace=True)

    # Optional: reorder for clarity
    cols = [
        "Gen ID", "Name","NZTM_X","NZTM_Y","POC code","GXP NZTM_X","GXP NZTM_Y", "Region", "North_South",
        "Capacity Multiplier", "Lower Bound", "Upper Bound", "Tech","Status"
    ]
    merged = merged[[c for c in cols if c in merged.columns]]

    # Export to Excel next to script (optional)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\subtransmission_with_names.xlsx")
    merged.to_excel(output_path, index=False)

    return merged

# Example direct run
if __name__ == "__main__":
    main_path =r"input file here"
    df_result = subtransmission_line(main_path)

    ref_path = r"reference file"
    def_result_names = subtransmission_line_names(df_result,ref_path)

    