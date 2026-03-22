import pandas as pd
import os


def summarise_transformers_upgrades(main_path):
    # Read Excel file
    df = pd.read_excel(main_path, sheet_name="Operating Units")

    # Make sure column names match your file; adjust if needed
    # Expecting columns like: 'ID', 'Capacity Multiplier', 'Upper Bound'
    if 'ID' not in df.columns:
        raise KeyError("Column 'ID' not found in the input file")

    # Filter IDs: length = 8, and last two digits = 53
    df_filtered = df[df['ID'].astype(str).apply(lambda x: len(x) == 8 and x[-2:] == '53')].copy()

    # Extract mid(3,3): positions 3–5 (Python index 2:5)
    df_filtered['POC ID'] = df_filtered['ID'].astype(str).str[2:5].astype(int)
    # Add Type column: Dem if POC ID > 300, Sup if < 300

    # Add Type column and adjust POC ID
    def classify_and_adjust(poc):
        if poc > 300:
            return poc - 300, 'Dem'
        else:
            return poc, 'Sup'

    df_filtered[['POC ID', 'Type']] = df_filtered['POC ID'].apply(lambda x: pd.Series(classify_and_adjust(x)))

    # Rename columns to match requested output
    df_result = df_filtered.rename(columns={
        'Upper Bound': 'Upgraded Capacity',
        'Capacity Multiplier': 'Capacity Multiplier'
    })[['POC ID', 'Upgraded Capacity', 'Capacity Multiplier', 'Type']]


    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\filtered_transformers_upgrade.xlsx")
    df_result.to_excel(output_path, index=False)


    return df_result

def enrich_with_poc_metadata(df_with_poc, ref_path):

    # Read the reference table
    ref = pd.read_excel(ref_path, sheet_name="GXP_edited_v4")

    # Handle column name variants and validate
    if 'descriptio' in ref.columns and 'description' not in ref.columns:
        ref = ref.rename(columns={'descriptio': 'description'})

    required_cols = ['Index', 'POC', 'North_South', 'Region', 'description', 'N Demand (MVA)', 'N Generation (MVA)',"NZTM_X","NZTM_Y"]
    missing = [c for c in required_cols if c not in ref.columns]
    if missing:
        raise KeyError(f"Missing columns in reference file: {missing}")

    # Ensure numeric join keys
    ref = ref.copy()
    ref['Index'] = pd.to_numeric(ref['Index'], errors='coerce').astype('Int64')

    df = df_with_poc.copy()
    df['POC ID'] = pd.to_numeric(df['POC ID'], errors='coerce').astype('Int64')

    # Merge on Index == POC ID
    merged = df.merge(
        ref[required_cols],
        left_on='POC ID',
        right_on='Index',
        how='left'
    ).drop(columns=['Index'])

    # Optional: reorder to surface the new fields next to POC ID
    cols_front = ['POC ID', 'POC', 'description',"NZTM_X","NZTM_Y", 'North_South', 'Region', 'N Demand (MVA)', 'N Generation (MVA)']
    rest = [c for c in merged.columns if c not in cols_front]
    merged = merged[cols_front + rest]


    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\filtered_transformers_with_POC_upgrades.xlsx")
    merged.to_excel(output_path, index=False)

    return merged



# Example direct run
if __name__ == "__main__":
    main_path =r"input file here"
    df_result = summarise_transformers_upgrades(main_path)
    # print(df_result)

    ref_path = r"ref file"
    df_result_2  = enrich_with_poc_metadata(df_result, ref_path)
    # print(df_result_2)
