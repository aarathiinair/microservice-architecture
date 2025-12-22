import pandas as pd
import json
from typing import Dict, Any, Set

# --- Configuration ---
# File containing the unique triggers found in the processed classification results
INPUT_FOUND_TRIGGERS_FILE = "trigger_name_counts.csv"
# File containing your master list of 50 (or more) triggers, including the sheet name
MASTER_TRIGGER_EXCEL_FILE = r"C:\Email_processing_demo\Control_Up_Trigger_List.xlsx" 
MASTER_TRIGGER_SHEET_NAME = "Categorized"
MASTER_TRIGGER_COLUMN = "TriggerName"

OUTPUT_REPORT_FILE = "master_trigger_audit_report.xlsx"

def audit_master_list():
    """Reads both files, compares the sets of triggers, and reports on missing ones."""
    
    # 1. Load the list of triggers found in the processed data (from the CSV)
    print(f"Loading triggers found in processed data from '{INPUT_FOUND_TRIGGERS_FILE}'...")
    try:
        df_found = pd.read_csv(INPUT_FOUND_TRIGGERS_FILE)
        # Assuming the column name in the CSV is 'Trigger Name' based on previous output
        found_triggers: Set[str] = set(df_found['Trigger Name'].unique())
        print(f"Total unique triggers found in processed data: {len(found_triggers)}")
        
    except FileNotFoundError:
        print(f"❌ Error: The found triggers file '{INPUT_FOUND_TRIGGERS_FILE}' was not found.")
        return
    except Exception as e:
        print(f"❌ Error loading found triggers data: {e}")
        return

    # 2. Load the master list of triggers (from the specified Excel sheet)
    print(f"Loading master trigger list from '{MASTER_TRIGGER_EXCEL_FILE}' (Sheet: '{MASTER_TRIGGER_SHEET_NAME}')...")
    
    try:
        # Read the specific sheet and column from your master Excel file
        df_master = pd.read_excel(MASTER_TRIGGER_EXCEL_FILE, sheet_name=MASTER_TRIGGER_SHEET_NAME)
        
        # Ensure the required column exists
        if MASTER_TRIGGER_COLUMN not in df_master.columns:
            print(f"❌ Error: Column '{MASTER_TRIGGER_COLUMN}' not found in sheet '{MASTER_TRIGGER_SHEET_NAME}'.")
            return

        # Get the set of unique triggers from your master list column, removing any NaNs
        master_triggers: Set[str] = set(df_master[MASTER_TRIGGER_COLUMN].dropna().astype(str).unique())
        print(f"Total unique triggers in master list: {len(master_triggers)}")
        
    except FileNotFoundError:
        print(f"❌ Error: The master Excel file '{MASTER_TRIGGER_EXCEL_FILE}' was not found.")
        return
    except Exception as e:
        print(f"❌ Error loading master list data: {e}")
        return

    # --- 3. Perform Set Comparison ---
    
    # Triggers in the master list that were **NOT** found in the processed data (i.e., the mismatch)
    master_triggers_not_in_data = master_triggers - found_triggers

    # --- 4. Generate Report and Export ---
    
    print("\n--- Audit Results ---")
    print(f"Master triggers NOT found in processed data: {len(master_triggers_not_in_data)}")
    
    # Create DataFrame for the report sheet
    df_master_missing = pd.DataFrame(
        list(master_triggers_not_in_data), 
        columns=['Master Trigger Name (Missing from Data)']
    )
    
    try:
        # Write the results to the new Excel file
        df_master_missing.to_excel(OUTPUT_REPORT_FILE, sheet_name='Missing_Master_Triggers', index=False)
            
        print(f"\n✅ Audit complete. The list of missing master triggers exported to '{OUTPUT_REPORT_FILE}'.")
        
    except Exception as e:
        print(f"\n❌ ERROR during report export: {e}")

if __name__ == '__main__':
    audit_master_list()