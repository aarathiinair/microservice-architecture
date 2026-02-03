import pandas as pd
from typing import Set

# --- Configuration ---
INPUT_CSV_FILE = r"C:\Email_processing_demo\end_triggered_list.xlsx"
OUTPUT_REPORT_FILE = "trigger_mismatch_report.xlsx"

# The column names from your uploaded CSV
ORIGINAL_COLUMN = "Original_Trigger_name"
DERIVED_COLUMN = "Derived_Trigger_Name"

def analyze_mismatches():
    """Reads the CSV, performs set comparisons, and reports the mismatches."""
    
    print(f"Loading data from '{INPUT_CSV_FILE}'...")
    
    try:
        # 1. Load the CSV file
        df = pd.read_excel(INPUT_CSV_FILE)
        
        # Ensure columns exist (case-sensitive check)
        if ORIGINAL_COLUMN not in df.columns or DERIVED_COLUMN not in df.columns:
            print(f"❌ Error: Expected columns '{ORIGINAL_COLUMN}' or '{DERIVED_COLUMN}' not found.")
            print(f"Found columns: {list(df.columns)}")
            return
            
        # 2. Extract unique triggers for comparison
        # Clean data: drop NaNs and convert to string to ensure consistency
        original_triggers: Set[str] = set(df[ORIGINAL_COLUMN].dropna().astype(str).str.strip().unique())
        derived_triggers: Set[str] = set(df[DERIVED_COLUMN].dropna().astype(str).str.strip().unique())
        
        print(f"Total Unique Original Triggers: {len(original_triggers)}")
        print(f"Total Unique Derived Triggers: {len(derived_triggers)}")
        
    except FileNotFoundError:
        print(f"❌ Error: The input file '{INPUT_CSV_FILE}' was not found.")
        return
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    # --- 3. Perform Set Comparisons ---
    
    # Mismatch A: Triggers that are ONLY in the Original list (were never derived/mapped to)
    original_only = original_triggers - derived_triggers
    
    # Mismatch B: Triggers that are ONLY in the Derived list (were not a source for any mapping)
    derived_only = derived_triggers - original_triggers

    # --- 4. Generate Report and Export ---
    
    print("\n--- Mismatch Analysis Results ---")
    print(f"Triggers ONLY in Original: {len(original_only)}")
    print(f"Triggers ONLY in Derived: {len(derived_only)}")
    
    # Create DataFrames for the report sheets
    df_original_only = pd.DataFrame(
        list(original_only), 
        columns=['Original Trigger Name (Unused in Derived)']
    )
    df_derived_only = pd.DataFrame(
        list(derived_only), 
        columns=['Derived Trigger Name (Not an Original Source)']
    )
    
    try:
        # Use pandas ExcelWriter to write both DataFrames to different sheets
        with pd.ExcelWriter(OUTPUT_REPORT_FILE, engine='openpyxl') as writer:
            df_original_only.to_excel(writer, sheet_name='A_Original_Only_Sources', index=False)
            df_derived_only.to_excel(writer, sheet_name='B_Derived_Only_Targets', index=False)
            
        print(f"\n✅ Analysis complete. Report exported to '{OUTPUT_REPORT_FILE}' with two sheets.")
        
    except Exception as e:
        print(f"\n❌ ERROR during report export: {e}")

if __name__ == '__main__':
    analyze_mismatches()