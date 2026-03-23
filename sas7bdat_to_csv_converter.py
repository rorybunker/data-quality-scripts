"""
SAS7BDAT to CSV Batch Converter
===============================

This script scans a directory for all `.sas7bdat` files, converts each file
to a CSV using `pandas`, and saves the output into a `csv_file_output/`
subdirectory created automatically if it does not already exist.

The script is intended for quick conversion of SAS datasets extracted from
analytics environments such as SAS EG, SAS Studio, or scheduled jobs.
It handles common encoding issues and ensures that byte-string columns
are correctly decoded to UTF‑8 before writing to CSV.

Features
--------
• Automatically detects all `.sas7bdat` files in the specified directory  
• Converts each file to CSV using `pandas.read_sas()`  
• Creates an output folder (`csv_file_output/`) inside the target directory  
• Attempts to decode byte-string columns to UTF‑8  
• Logs errors per file without stopping the whole batch run  

Function
--------
convert_sas7bdat_to_csv(directory)
    Converts all SAS7BDAT files within the given directory to CSV format.

    Parameters
    ----------
    directory : str
        Path to the directory containing `.sas7bdat` files.

Output
------
For each SAS file found:
    • A CSV file written to:  
      <directory>/csv_file_output/<filename>.csv

If decoding or reading fails for a file, the script prints an error but
continues processing other files.

Usage
-----
Run the script directly (e.g., double‑click or via terminal):

    python convert_sas.py

Or import and call the function manually:

    from convert_sas import convert_sas7bdat_to_csv
    convert_sas7bdat_to_csv("/path/to/sas/files")

Notes
-----
• Requires pandas with SAS support (pyreadstat backend).  
• Ensures byte-type columns are safely decoded to UTF‑8.  
• Designed for batch processing in ETL/data migration workflows.

"""

import pandas as pd
import os
import glob

def convert_sas7bdat_to_csv(directory):
    """Converts all SAS7BDAT files in a given directory to CSV format and saves them to a subdirectory.

    Args:
        directory: The path to the directory containing SAS7BDAT files.
    """
    # Define the output directory
    output_directory = os.path.join(directory, "csv_file_output")

    # Create the output directory if it does not already exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    for file in glob.glob(os.path.join(directory, "*.sas7bdat")):
        try:
            # Read the SAS file into a DataFrame
            df = pd.read_sas(file, encoding='utf-8')

            # Decode byte strings
            for col in df.select_dtypes(include=[object]).columns:
                if df[col].apply(lambda x: isinstance(x, bytes)).any():
                    df[col] = df[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

            # Define the CSV file path
            csv_file = os.path.join(output_directory, os.path.basename(file).replace('.sas7bdat', '.csv'))

            # Convert the DataFrame to CSV
            df.to_csv(csv_file, index=False)

        except Exception as e:
            print(f"Error processing file {file}: {e}")

# Get the directory of the current script
script_directory = os.path.dirname(os.path.abspath(__file__))

# Call the function with the script's directory
convert_sas7bdat_to_csv(script_directory)
