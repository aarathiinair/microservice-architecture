import sys
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from typing import Iterator
from app.db_functions.db_schema2 import get_db, Base 
from app.db_functions.db_schema2 import Server

# Assuming the project structure allows importing from app.db and app.models
# try:

# except ImportError:
#     print("Error: Could not import necessary modules from 'app'.")
#     print("Please ensure your Python path is set correctly or run this script from the project root.")
#     sys.exit(1)

# --- Configuration ---
# IMPORTANT: Update this path to your actual Excel file containing the server data
EXCEL_FILE_PATH = r"app/Server_Overview.xlsx" 

# Expected column headers in the Excel sheets
COLUMN_MAPPING = {
    "Computername": "computername",
    "Description/Function": "description_function",
    "Responsible Person": "responsible_person",
}
# --- End Configuration ---


def get_server_data_from_excel(file_path: str) -> Iterator[Server]:
    """
    Reads data from all sheets in the Excel file and yields Server objects.
    Each sheet name is used as the 'Group' value.
    """
    print(f"Reading data from {file_path}...")
    
    try:
        # Read all sheets into an OrderedDict of DataFrames
        xls = pd.ExcelFile(file_path)
        all_sheets_data = pd.read_excel(xls, sheet_name=None) 
    except FileNotFoundError:
        print(f"Error: Excel file not found at path: {file_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        sys.exit(1)

    total_rows = 0
    
    for group_name, df in all_sheets_data.items():
        print(f"Processing sheet: '{group_name}'")
        
        # Clean column names for mapping
        df.columns = df.columns.str.strip()
        
        # Ensure required columns exist
        required_cols = list(COLUMN_MAPPING.keys())
        if not all(col in df.columns for col in required_cols):
            print(f"Skipping sheet '{group_name}': Missing one or more required columns: {required_cols}")
            continue

        # Rename columns to match the SQLAlchemy model attributes
        df = df.rename(columns=COLUMN_MAPPING)
        
        # Select only the relevant columns and convert to records
        for _, row in df.iterrows():
            # Check for NaN or empty computername to skip invalid rows
            if pd.isna(row.get('computername')) or not str(row['computername']).strip():
                continue
                
            total_rows += 1
            
            # Create a Server object
            server = Server(
                computername=str(row['computername']).strip(),
                group=group_name.strip(),
                description_function=str(row.get('description_function', '')).strip(),
                responsible_person=str(row.get('responsible_person', '')).strip(),
            )
            yield server
    
    print(f"Total valid server records prepared across all sheets: {total_rows}")


def populate_servers_table(file_path: str):
    """
    Connects to the database and inserts the server data.
    Allows duplicate computer names as per user request.
    """
    db_generator = get_db()
    db: Session = next(db_generator) 

    try:
        # 1. Ensure the 'servers' table exists
        engine = db.get_bind()
        print(f"Attempting to create table '{Server.__tablename__}' if it does not exist...")
        Base.metadata.create_all(bind=engine)
        print("Table structure ensured.")

        print("Starting data insertion (allowing duplicates)...")
        
        server_records = list(get_server_data_from_excel(file_path))
        
        if server_records:
            # Add all records and commit in a single transaction
            db.add_all(server_records)
            db.commit()
            print("-" * 40)
            print(f"Database population complete.")
            print(f"Total records inserted: {len(server_records)}")
            print("-" * 40)
        else:
            print("No valid records found to insert.")

    except SQLAlchemyError as e:
        db.rollback()
        print(f"\nDatabase error occurred: {e}")
        print("The transaction was rolled back.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    finally:
        db.close()
        print("Database session closed.")

if __name__ == "__main__":
    populate_servers_table(EXCEL_FILE_PATH)
    print("--- Script Finished ---")