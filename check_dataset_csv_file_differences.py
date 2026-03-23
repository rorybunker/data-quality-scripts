
"""
CSV Dataset Comparison Utility
==============================

This script compares two CSV extracts (typically SQL outputs) and identifies:

    • Rows that exist only in the first dataset
    • Rows that exist only in the second dataset
    • Rows with matching join keys but different column values
    • (Optional) Differences in column order between the two files

It is designed for validating whether two SQL outputs (e.g., old vs new logic)
produce the same results, independent of column ordering—unless explicitly requested.

Usage
-----

Basic comparison (ignores column order):
    python compare.py file1.csv file2.csv --key ref_org_id

Multi‑column join key:
    python compare.py file1.csv file2.csv --key ref_org_id activity_start_date

Enable column‑order comparison:
    python compare.py file1.csv file2.csv --key ref_org_id --check-column-order

Arguments
---------

file1 : str  
    Path to the first CSV dataset.

file2 : str  
    Path to the second CSV dataset.

--key : list[str], required  
    One or more column names used as the join key(s).

--check-column-order : flag (default: off)  
    If provided, compares the order of columns in the two CSV files.

Outputs
-------

The script will write the following files if differences are found:

    • only_in_<file1>.csv          – rows found only in file1
    • only_in_<file2>.csv          – rows found only in file2
    • value_differences.csv        – rows with matching keys but mismatched values
    • column_order_differences.csv – (if enabled) differences in column positions

Notes
-----

• Column order is ignored by default because value comparison relies on column names.
• Whitespace trimming is performed to avoid false mismatches.
• This script is safe for large datasets, but extremely large files may require chunked loading.

"""


import pandas as pd
import argparse

def compare_csv_datasets(file1, file2, join_keys, check_column_order=False):
    """
    Compare two CSV datasets and identify:
      - rows only in file1
      - rows only in file2
      - rows with differing column values (same keys)
      - optionally compare column order

    Args:
        file1 (str): Path to first CSV
        file2 (str): Path to second CSV
        join_keys (list[str]): Column(s) to join on
        check_column_order (bool): Whether to compare column order
    """

    print("🔄 Loading CSV files...")
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Convert join keys to list if needed
    if isinstance(join_keys, str):
        join_keys = [join_keys]

    # ----------------------------
    # ✅ OPTIONAL COLUMN ORDER CHECK
    # ----------------------------
    if check_column_order:
        print("\n📏 Checking column order...")

        cols1 = list(df1.columns)
        cols2 = list(df2.columns)

        if cols1 == cols2:
            print("✅ Column order matches exactly.")
        else:
            print("⚠️ Column order differs!")

            # Find columns in order they appear and differences
            max_len = max(len(cols1), len(cols2))
            differences = []

            for i in range(max_len):
                col1 = cols1[i] if i < len(cols1) else "<missing>"
                col2 = cols2[i] if i < len(cols2) else "<missing>"
                if col1 != col2:
                    differences.append((i, col1, col2))

            print("\nDifferences (position, file1_col, file2_col):")
            for pos, c1, c2 in differences:
                print(f" - Position {pos}: {c1}  !=  {c2}")

            # Export differences
            diff_export = pd.DataFrame(differences, columns=["Position", "File1_Column", "File2_Column"])
            diff_export.to_csv("column_order_differences.csv", index=False)
            print("✅ Exported column order differences to 'column_order_differences.csv'")

    print("\n🔧 Normalising string fields...")
    df1 = df1.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df2 = df2.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # -----------------------------------
    # ✅ Rows only in file1
    # -----------------------------------
    print("\n🔍 Finding rows only in file1...")
    only_in_1 = (
        df1.merge(df2, on=join_keys, how="left", indicator=True)
        .query("_merge == 'left_only'")
        .drop(columns=["_merge"])
    )

    print(f"✅ Rows only in {file1}: {len(only_in_1)}")
    if not only_in_1.empty:
        out1 = f"only_in_{file1}.csv"
        only_in_1.to_csv(out1, index=False)
        print(f"📄 Exported to {out1}")

    # -----------------------------------
    # ✅ Rows only in file2
    # -----------------------------------
    print("\n🔍 Finding rows only in file2...")
    only_in_2 = (
        df2.merge(df1, on=join_keys, how="left", indicator=True)
        .query("_merge == 'left_only'")
        .drop(columns=["_merge"])
    )

    print(f"✅ Rows only in {file2}: {len(only_in_2)}")
    if not only_in_2.empty:
        out2 = f"only_in_{file2}.csv"
        only_in_2.to_csv(out2, index=False)
        print(f"📄 Exported to {out2}")

    # -----------------------------------
    # ✅ Value differences for matched keys
    # -----------------------------------
    print("\n🔍 Checking for mismatched values...")

    merged = df1.merge(df2, on=join_keys, how="inner", suffixes=("_old", "_new"))

    diff_records = []

    for col in df1.columns:
        if col in join_keys:
            continue

        col_old = col + "_old"
        col_new = col + "_new"

        if col_old in merged.columns and col_new in merged.columns:
            diffs = merged[merged[col_old] != merged[col_new]]
            if not diffs.empty:
                diff_records.append((col, len(diffs)))

    if diff_records:
        print("\n⚠️ Columns with mismatched values:")
        for col, count in diff_records:
            print(f" - {col}: {count} differences")

        merged.to_csv("value_differences.csv", index=False)
        print("📄 Exported value differences to 'value_differences.csv'")
    else:
        print("✅ No row-level value differences detected.")

    print("\n✅ Comparison complete.")


# ----------------------
# ✅ Argument Parsing
# ----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare two CSV exports from SQL")

    parser.add_argument("file1", help="Path to first CSV file")
    parser.add_argument("file2", help="Path to second CSV file")
    parser.add_argument("--key", required=True, nargs="+", help="Join key(s) for comparison")
    parser.add_argument("--check-column-order", action="store_true",
                        help="Enable checking column ordering (default: off)")

    args = parser.parse_args()

    compare_csv_datasets(
        file1=args.file1,
        file2=args.file2,
        join_keys=args.key,
        check_column_order=args.check_column_order
    )
