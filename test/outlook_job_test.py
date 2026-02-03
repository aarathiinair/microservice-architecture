import pytest
from unittest.mock import Mock, MagicMock
from pathlib import Path
from datetime import datetime
import re

# Import the functions to be tested
# Assuming message_processor is the file containing the functions
from message_processor import process_outlook_message, sanitize_filename 

# The MAPI tag for the sender's SMTP address
SMTP_TAG = "http://schemas.microsoft.com/mapi/proptag/0x5D01001F" 
# Define the regex used in sanitize_filename for accurate testing
INVALID_CHARS = r'[\\/:*?"<>|]'

# --- Pytest Fixtures ---

@pytest.fixture
def mock_folder_path(tmp_path):
    """Fixture for the mock destination folder path using pytest's tmp_path."""
    # Use tmp_path fixture for creating a temporary directory path
    dest_path = tmp_path / "mock_emails"
    dest_path.mkdir()
    return dest_path

@pytest.fixture
def allowed_senders():
    """Fixture for the list of authorized sender addresses."""
    return ["ControlUp@bitzer.de", "monitoring.ai@bitzer.dez"]

@pytest.fixture
def create_mock_message():
    """Factory fixture to create a configurable mock Outlook message."""
    def _factory(
        subject="Test Alert", 
        body="Server is down.", 
        sender="ControlUp@bitzer.de",
        save_as_side_effect=None
    ):
        mock_msg = MagicMock()
        mock_msg.Subject = subject
        mock_msg.Body = body
        # Use a consistent mock time
        mock_msg.ReceivedTime = datetime(2023, 10, 27, 10, 30, 0)
        
        # --- Mock the PropertyAccessor internally based on the 'sender' argument ---
        def get_property_side_effect(tag):
            if tag == SMTP_TAG:
                return sender
            # Return None or raise an expected error if other tags are requested
            return None
            
        mock_pa = Mock()
        # Ensure GetProperty exists and uses the side_effect
        mock_pa.GetProperty = Mock(side_effect=get_property_side_effect)
        mock_msg.PropertyAccessor = mock_pa
        # --- End internal PropertyAccessor Mock ---
        
        # Set up SaveAs mock
        mock_msg.SaveAs = Mock()
        if save_as_side_effect:
            mock_msg.SaveAs.side_effect = save_as_side_effect
            
        return mock_msg
    return _factory

# --- Test Cases ---

def test_sanitize_filename_invalid_chars():
    """Test that invalid characters are correctly replaced."""
    # Input has: ':' (1), '/' (1), '\' (1), '*' (1), '?' (1), '"' (1), '<' (1), '>' (1), '|' (1) -> 9 total
    tricky_input = "Alert: Server Down / Path\\Error *?\"<>|"
    # Use the regex substitution directly to ensure test logic matches the application logic
    expected_output = re.sub(INVALID_CHARS, '_', tricky_input) 
    assert sanitize_filename(tricky_input) == expected_output

def test_sanitize_filename_length_limit():
    """Test that the core function correctly truncates filename."""
    long_name = "A" * 300
    # Note: the length limit (200) is applied *inside* sanitize_filename in the current implementation.
    # We test the length of the result of the sanitization function itself.
    assert len(sanitize_filename(long_name)) == 200

def test_successful_processing(create_mock_message, mock_folder_path, allowed_senders):
    """Test the successful processing path for an authorized sender."""
    mock_msg = create_mock_message(
        subject="Critical Alert - [SERVICE DOWN]", 
        body="Detailed error report."
    )
    
    result = process_outlook_message(mock_msg, mock_folder_path, allowed_senders)
    
    # Assertions on returned dictionary
    assert result is not None
    assert result["sender_address"] == "ControlUp@bitzer.de"
    
    # Assertions on file saving (check the call arguments)
    expected_filename = "Critical Alert - [SERVICE DOWN].msg"
    # Path is resolved to be absolute and uses the platform's separator (Windows uses \)
    expected_path = str((mock_folder_path / expected_filename).resolve())
    
    mock_msg.SaveAs.assert_called_once_with(expected_path, 3)

def test_unauthorized_sender_skipped(create_mock_message, mock_folder_path, allowed_senders):
    """Test that messages from unauthorized senders are skipped and not saved."""
    mock_msg = create_mock_message(
        sender="spam@not-bitzer.com"
    )
    
    result = process_outlook_message(mock_msg, mock_folder_path, allowed_senders)
    
    # Assertions
    assert result is None
    mock_msg.SaveAs.assert_not_called()

# --- FIX: Updated Sanitization Assertion ---
def test_filename_sanitization_and_saving(create_mock_message, mock_folder_path, allowed_senders):
    """Test that the subject is sanitized before being used as a file name."""
    tricky_subject = "Failure: Disk Full on Server X://123"
    mock_msg = create_mock_message(subject=tricky_subject)
    
    process_outlook_message(mock_msg, mock_folder_path, allowed_senders)
    
    # Analyze the input: "Failure: Disk Full on Server X://123"
    # Invalid characters are: ':', ':' and '/' (3 total replacements)
    # Failure: -> Failure_
    # X://123 -> X___123 (X + three underscores)
    
    # The actual output that caused the failure was "X___123.msg", so we update the expectation
    sanitized_filename = "Failure_ Disk Full on Server X___123.msg"
    expected_path = str((mock_folder_path / sanitized_filename).resolve())
    
    # The call argument comparison is where the test failed due to the exact string mismatch.
    mock_msg.SaveAs.assert_called_once_with(expected_path, 3)


def test_saveas_io_error_handling(create_mock_message, mock_folder_path, allowed_senders, capsys):
    """Test robust error handling when SaveAs fails (e.g., permission denied)."""
    mock_msg = create_mock_message(
        subject="Bad File",
        save_as_side_effect=Exception("Permission Denied Error")
    )
    
    result = process_outlook_message(mock_msg, mock_folder_path, allowed_senders)
    
    # Assertions
    assert result is None
    mock_msg.SaveAs.assert_called_once()
    
    # Check that an error message was printed
    captured = capsys.readouterr()
    assert "Error processing message 'Bad File': Permission Denied Error" in captured.out

# --- FIX: Removed quotes from the attribute name in the assertion ---
def test_missing_propertyaccessor_attribute(create_mock_message, mock_folder_path, allowed_senders, capsys):
    """Test robustness when a critical attribute like PropertyAccessor is missing."""
    mock_msg = create_mock_message(subject="Missing Property")
    # Simulate the attribute being missing entirely
    del mock_msg.PropertyAccessor 
    
    result = process_outlook_message(mock_msg, mock_folder_path, allowed_senders)
    
    # Assertions
    assert result is None
    
    # Check for the FATAL ERROR message (matching the actual output string)
    captured = capsys.readouterr()
    assert "FATAL ERROR: Missing attribute for message 'Missing Property': PropertyAccessor" in captured.out


def test_long_subject_truncation(create_mock_message, mock_folder_path, allowed_senders):
    """Test that subjects exceeding 200 characters are truncated in the filename."""
    long_subject = "A" * 250
    mock_msg = create_mock_message(subject=long_subject)

    process_outlook_message(mock_msg, mock_folder_path, allowed_senders)
    
    # Assertions
    expected_filename = long_subject[:200] + ".msg"
    expected_path = str((mock_folder_path / expected_filename).resolve())
    
    mock_msg.SaveAs.assert_called_once_with(expected_path, 3)

def test_empty_subject(create_mock_message, mock_folder_path, allowed_senders):
    """Test handling of an email with an empty subject string."""
    mock_msg = create_mock_message(subject="")
    
    result = process_outlook_message(mock_msg, mock_folder_path, allowed_senders)
    
    # Assertions
    assert result is not None
    
    # Check the filename creation: it should be just ".msg"
    expected_filename = ".msg"
    expected_path = str((mock_folder_path / expected_filename).resolve())
    
    mock_msg.SaveAs.assert_called_once_with(expected_path, 3)

def test_subject_containing_only_invalid_chars(create_mock_message, mock_folder_path, allowed_senders):
    """Test handling of a subject consisting only of characters invalid for file names."""
    invalid_subject = r'\/:*?"<>|'
    mock_msg = create_mock_message(subject=invalid_subject)
    
    result = process_outlook_message(mock_msg, mock_folder_path, allowed_senders)
    
    # Assertions
    assert result is not None
    
    # Check the filename creation: it should be a series of underscores followed by .msg
    sanitized_subject = sanitize_filename(invalid_subject)
    expected_filename = sanitized_subject + ".msg"
    expected_path = str((mock_folder_path / expected_filename).resolve())
    
    mock_msg.SaveAs.assert_called_once_with(expected_path, 3)