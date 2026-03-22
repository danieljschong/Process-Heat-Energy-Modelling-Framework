import pandas as pd
import os


def distribution_capacity(filepath):
    """
    Reads 'Operating Units' sheet, filters by ID, and returns specific columns.
    
    Parameters:
        filepath (str): Full path to the Excel file.

    Returns:
        pd.DataFrame: Filtered DataFrame with Fac ID, Capacity Multiplier, Lower Bound, Upper Bound.
    """
    # Load the Excel sheet
    df = pd.read_excel(filepath, sheet_name="Operating Units")

    # Ensure 'ID' is string
    df['ID'] = df['ID'].astype(str)

    # Filter: ID starts with 'O3' and ID[5:8] == '103'
    filtered = df[df['ID'].str.startswith("O3") & (df['ID'].str[5:8] == "103")].copy()

    # Extract 'Fac ID' as integer from ID[2:5]
    filtered.loc[:, 'Fac ID'] = filtered['ID'].str[2:5].astype(int)

    # Drop 'ID', 'Lower Bound', and 'Upper Bound' (per instructions)
    filtered.drop(columns=['ID'], inplace=True, errors='ignore')
    filtered.drop(columns=['Proportional Cost','Lower Bound', 'Upper Bound'], inplace=True, errors='ignore')

    # Aggregation logic
    agg_funcs = {
        'Fix Cost': 'sum',
        'Capacity Multiplier': 'max',
        'Cost': 'sum'
    }

    # Perform aggregation
    aggregated = filtered.groupby('Fac ID', as_index=False).agg(agg_funcs)

    # Reorder columns
    desired_order = [
        'Fac ID',
        'Fix Cost',
        'Capacity Multiplier',
        'Cost'
    ]
    result = aggregated[desired_order]

    # Save to Excel in the script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\distribution.xlsx")
    result.to_excel(output_path, index=False)

    return result

def distribution_capacity_fac_names(df_result, ref_path):
    """
    Merges df_result with factory names based on Fac ID.

    Parameters:
        df_result (pd.DataFrame): DataFrame with Fac ID column.
        ref_path (str): Path to reference Excel file with factory info.

    Returns:
        pd.DataFrame: Merged DataFrame with added 'Company Name' column.
    """
    # Read Factory sheet from reference file
    df_factory = pd.read_excel(ref_path, sheet_name="Factory_updated (5)")

    # Ensure ObjectID is int for proper matching
    df_factory['ObjectID'] = df_factory['ObjectID'].astype(int)

    # Merge on Fac ID <-> ObjectID
    merged = pd.merge(df_result, df_factory[['ObjectID', 'Company name',"Plant site","NZTM_X",	"NZTM_Y",	"POC", "GXP NZTM_X",	"GXP NZTM_Y",	 "North_South" ]],
                      left_on='Fac ID', right_on='ObjectID', how='left')


    # Drop redundant ObjectID column
    merged.drop(columns=['ObjectID'], inplace=True)

    # Desired column order
    desired_order = ['Fac ID','Company name',"Plant site","NZTM_X",	"NZTM_Y",	"POC", "GXP NZTM_X",	"GXP NZTM_Y",	 "North_South",'Fix Cost','Capacity Multiplier','Cost']    
    
    final = merged[[col for col in desired_order if col in merged.columns]].copy()

    # Save to Excel in the script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, r"Organised_Spreadsheets\distribution_with_names.xlsx")
    final.to_excel(output_path, index=False)    
    return final

# Example direct run
if __name__ == "__main__":

    main_path = r"your input path"
    df_result = distribution_capacity(main_path)
    # print(df_result)

    ref_path = r"ref file path"
    def_result_names = distribution_capacity_fac_names(df_result,ref_path)
    
