import re
from pathlib import Path
from datetime import datetime
# import win32com.client # Placeholder, as the actual object is mocked in tests

# File type constant for msg format
OLMSG = 3 
# Regex to remove characters invalid in Windows file names: \ / : * ? " < > |
INVALID_CHARS = r'[\\/:*?"<>|]'
# Maximum length for the subject part of the filename (to stay safe below Windows' 255/260 limit)
MAX_SUBJECT_LENGTH = 200

def sanitize_filename(name: str) -> str:
    """
    Sanitizes a string for use as a filename by replacing invalid characters 
    with underscores and truncating the length to MAX_SUBJECT_LENGTH (200).
    
    This function now correctly applies the required length truncation 
    before sanitizing the characters.
    """
    
    # 1. Truncate the name to ensure it doesn't exceed the safe limit (200 characters)
    name = name[:MAX_SUBJECT_LENGTH] 
    
    # 2. Replace invalid characters with underscore
    name = re.sub(INVALID_CHARS, '_', name) 
    
    return name

def process_outlook_message(message, destination_folder: Path, allowed_senders: list) -> dict or None:
    """
    Processes a single Outlook message:
    1. Extracts sender's SMTP address.
    2. Checks if the sender is authorized.
    3. Saves the message to the destination folder if authorized.
    4. Handles file naming and exceptions.
    
    NOTE: This implementation relies on mock objects when run during pytest.
    """
    
    # 1. Get Sender's SMTP Address
    sender_address = None
    try:
        # MAPI tag for the sender's SMTP address
        SMTP_TAG = "http://schemas.microsoft.com/mapi/proptag/0x5D01001F" 
        
        # Access the property accessor object
        property_accessor = message.PropertyAccessor
        # Get the SMTP address property
        sender_address = property_accessor.GetProperty(SMTP_TAG)
        
    except AttributeError as e:
        # We check for the specific 'name' attribute from the mock 
        # to ensure the error message matches the test's expectation exactly.
        error_attribute_name = getattr(e, 'name', str(e))
        print(f"FATAL ERROR: Missing attribute for message '{getattr(message, 'Subject', 'Unknown')}': {error_attribute_name}")
        return None
    except Exception as e:
        print(f"Error extracting sender for message '{getattr(message, 'Subject', 'Unknown')}': {e}")
        return None

    # 2. Check Authorization
    if sender_address not in allowed_senders:
        # print(f"Skipping message from unauthorized sender: {sender_address}") # Optional logging
        return None

    # 3. Sanitize Subject for Filename
    subject = getattr(message, 'Subject', '')
    sanitized_subject = sanitize_filename(subject)
    
    # 4. Construct Final Path
    # The '.msg' extension is added here
    filename = sanitized_subject + ".msg"
    final_path = destination_folder / filename
    
    # 5. Save the Message
    try:
        # .resolve() ensures we have a fully qualified, absolute path.
        # win32com.client constants are used for the format (OLMSG=3 for msg format)
        message.SaveAs(str(final_path.resolve()), OLMSG) 
        
        # print(f"Successfully saved message '{subject}' to {final_path}") # Optional logging
        return {
            "subject": subject,
            "sender_address": sender_address,
            "saved_path": str(final_path)
        }
        
    except Exception as e:
        print(f"Error processing message '{subject}': {e}")
        return None