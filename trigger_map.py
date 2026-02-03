import pandas as pd
import json
from typing import Dict, Any

# --- Configuration ---
INPUT_FILENAME = r"C:\Email_processing_demo\output_filename.xlsx"
OUTPUT_FILENAME = "trigger_name_counts.csv"

def extract_trigger_name(json_string: str) -> str:
    """
    Parses a JSON string and extracts the 'trigger_name' field.
    Returns 'PARSING_ERROR' if the string is invalid JSON or the key is missing.
    """
    try:
        # Load the JSON string into a Python dictionary
        data: Dict[str, Any] = json.loads(json_string)
        
        # Return the value of the 'trigger_name' key
        # We use .get() to safely handle cases where the key might be missing
        return data.get("trigger_name", "MISSING_TRIGGER_NAME")
    except json.JSONDecodeError:
        # Handle cases where the cell content is not valid JSON
        return "JSON_DECODE_ERROR"
    except Exception:
        # Catch any other unexpected errors
        return "UNKNOWN_ERROR"

def analyze_triggers():
    """Reads the Excel data, extracts trigger names, and calculates counts."""
    
    print(f"Loading data from '{INPUT_FILENAME}'...")
    
    try:
        # 1. Read the Excel file into a DataFrame
        df = pd.read_excel(INPUT_FILENAME)
    except FileNotFoundError:
        print(f"❌ Error: The input file '{INPUT_FILENAME}' was not found. Please run excel_exporter.py first.")
        return
    except Exception as e:
        print(f"❌ Error loading Excel file: {e}")
        return

    # 2. Apply the parsing function to the entire 'Output' column
    print("Parsing JSON in 'Output' column and extracting trigger names...")
    
    # We use the .apply() method to run our custom function row-by-row on the column.
    df['Extracted Trigger Name'] = df['Output'].apply(extract_trigger_name)

    # 3. Calculate the unique counts
    # The .value_counts() method is the perfect tool for this aggregation.
    trigger_counts_series = df['Extracted Trigger Name'].value_counts()

    # Convert the resulting Series into a DataFrame for better display and export
    counts_df = trigger_counts_series.reset_index()
    counts_df.columns = ['Trigger Name', 'Count']
    
    print("\n--- Unique Trigger Name Counts ---")
    print(counts_df)
    
    # 4. Export the results to a new CSV file
    counts_df.to_csv(OUTPUT_FILENAME, index=False)
    print(f"\n✅ Analysis complete. Counts exported to '{OUTPUT_FILENAME}'")


if __name__ == '__main__':
    analyze_triggers()