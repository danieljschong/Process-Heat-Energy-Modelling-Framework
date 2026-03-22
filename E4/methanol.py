import pandas as pd
from pathlib import Path


def summarize_generators_by_key(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path, sheet_name="Operating Units")

    if not {"ID", "Capacity Multiplier"}.issubset(df.columns):
        raise KeyError("Expected columns 'ID' and 'Capacity Multiplier'")

    s = df["ID"].astype(str)

    # Filter IDs: length 9, start O1, end 280
    mask = (s.str.len() == 9) & s.str.startswith("O1") & s.str.endswith("280")

    df_filtered = df.loc[mask, ["ID", "Capacity Multiplier"]].copy()

    # Extract identifier between O1 and 280
    df_filtered["Identifier"] = pd.to_numeric(
        df_filtered["ID"].str.slice(2, 6),
        errors="coerce"
    ).astype("Int64")

    df_filtered = df_filtered.drop(columns=["ID"])

    summary = (
        df_filtered.groupby("Identifier", as_index=False)["Capacity Multiplier"]
        .sum()
    )

    return summary


def methanol_reference(ref_path: str) -> pd.DataFrame:
    ref_df = pd.read_excel(ref_path, sheet_name="Generation_updated_v1")

    required = {
        "ObjectID",
        "Types of power station",
        "Type",
        "Name",
        "Status",
        "NZTM_X",
        "NZTM_Y",
        "Region",
        "North_South",
    }

    if not required.issubset(ref_df.columns):
        missing = required - set(ref_df.columns)
        raise KeyError(f"Reference file missing expected columns: {missing}")

    ref_df = ref_df[
        [
            "ObjectID",
            "Types of power station",
            "Type",
            "Name",
            "Status",
            "NZTM_X",
            "NZTM_Y",
            "Region",
            "North_South",
        ]
    ].copy()

    return ref_df


if __name__ == "__main__":

    main_path = r"main file"
    ref_path = r"ref file"

    summary_df = summarize_generators_by_key(main_path)
    ref_df = methanol_reference(ref_path)

    summary_df["Identifier"] = pd.to_numeric(summary_df["Identifier"], errors="coerce").astype("Int64")
    ref_df["ObjectID"] = pd.to_numeric(ref_df["ObjectID"], errors="coerce").astype("Int64")

    merged_df = summary_df.merge(
        ref_df,
        left_on="Identifier",
        right_on="ObjectID",
        how="left"
    )

    merged_df = merged_df.drop(columns=["ObjectID"])

    # Keep only rows where Capacity Multiplier is not 0
    merged_df = merged_df[merged_df["Capacity Multiplier"] != 0]

    # Output folder
    script_dir = Path(__file__).resolve().parent
    out_dir = script_dir / "Organised_Spreadsheets"
    out_dir.mkdir(parents=True, exist_ok=True)

    output_path = out_dir / "methanol_results.xlsx"

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        merged_df.to_excel(writer, sheet_name="Merged", index=False)

    print(f"Saved to: {output_path}")