import re 
import pandas as pd 
import os 
# from control_parse import EmailData

class EmailParser:
    def __init__(self, excel_path="incident_log.xlsx"):
        self.excel_path = excel_path
    
    def parse_email(self, email: dict) -> dict:
        """
        Parse ControlUp email content and extract key fields
        Returns a dictionary with all parsed fields for flexibility
        """
        # Get email content and subject
        email_body = email.get("content", "")
        email_subject = email.get("subject", "")
        
        
        # Normalize escaped line breaks if needed
        if "\\n" in email_body:
            email_body = email_body.encode().decode('unicode_escape')
        
        # Extract key-value pairs using regex
        matches = re.findall(r"^\s*(.+?):\s*(.+$)", email_body, re.MULTILINE)
        parsed_data = {key.strip().lower(): value.strip() for key, value in matches}

        # Get all lines for primary reason extraction
        lines = [l.strip() for l in email_body.splitlines() if l.strip()]
    
        # Extract the "primary reason" (first non-empty line that is not key:value)
        primary_reason = None
        for line in lines:
            # Skip empty lines
            if not line:
                continue
            # If line contains ":" it's a key-value pair, skip it
            if ":" in line:
                continue
            # This is the primary reason line
            primary_reason = line
            break
        
        # Add subject to parsed data
        parsed_data["subject"] = email_subject
        parsed_data["primary_reason"] = primary_reason
        parsed_data["recived_time"] = email.get("received_time", "")
        parsed_data["sender_address"] = email.get("sender_address", "")
        parsed_data["content"] = email_body
        
        # Handle the resource name -> computer name mapping
        if 'resource name' in parsed_data:
            parsed_data['computer name'] = parsed_data['resource name']
        
        # Debug print to see what we extracted
        print(f"  Debug - Extracted fields: {list(parsed_data.keys())}")
        print(f"  Debug - Trigger name: {parsed_data.get('trigger name', 'NOT FOUND')}")
        print(f"  Debug - Computer name: {parsed_data.get('computer name', 'NOT FOUND')}")
        print(f"  Debug - Primary reason: {primary_reason}")
        
        return parsed_data
    
    def get_deduplication_fields(self, email: dict) -> dict:
        """
        Extract only the fields needed for deduplication
        Returns a dictionary with the three key fields: trigger_name, computer_name, subject
        """
        parsed_data = self.parse_email(email)
        
        return {
            'trigger_name': parsed_data.get('trigger name', ''),
            'computer_name': parsed_data.get('computer name', ''),
            'subject': parsed_data.get('subject', ''),
            'body':parsed_data.get('content','')
        }


    
    def append_to_excel(self, parsed_data: dict):
        """Append parsed data to Excel file (keeping original functionality)"""
        df = pd.DataFrame([parsed_data])
        print("df columns:", df.columns.tolist())
        
        if 'resource name' in df.columns:
            df.rename(columns={'resource name': 'computer name'}, inplace=True) 
        
        if os.path.exists(self.excel_path):
            existing_df = pd.read_excel(self.excel_path)

            # Add missing columns to existing df
            for col in df.columns:
                if col not in existing_df.columns:
                    existing_df[col] = None
            # Add missing columns to new row
            for col in existing_df.columns:
                if col not in df.columns:
                    df[col] = None
            updated_df = pd.concat([existing_df, df], ignore_index=True)
        else:
            updated_df = df
        
        updated_df.to_excel(self.excel_path, index=False)
        
    def process_email(self, email_body: str):
        """Legacy method - keeping for compatibility"""
        parsed_data = self.parse_email(email_body)
        print(parsed_data)
        return parsed_data

# Test code (only runs when this file is executed directly)
if __name__ == "__main__":
    # Sample email content for testing
    email_content = """Organization Name: Bitzer
Trigger name: CITRIX PVS Service up
Process name: SoapServer.exe
Process ID: 3992
User name: BITZER\\s00228
Session ID: 0
Computer name: DESON01057
Incident timestamp (UTC +2 W. Europe Standard Time): 8/27/2025 8:29:21 AM
"""

    # Test the parser
    parser = EmailParser()
    email_dict = {
        "content": email_content,
        "subject": "Test ControlUp Alert"
    }
    
    print("Testing email parser...")
    result = parser.get_deduplication_fields(email_dict)
    print("Deduplication fields:", result)
