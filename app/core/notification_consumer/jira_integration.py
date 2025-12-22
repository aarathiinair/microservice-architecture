import logging
import asyncio
import aiohttp
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field, fields
from concurrent.futures import ThreadPoolExecutor
import io
from email.message import EmailMessage
from jira import JIRA
from app.config import settings
from pathlib import Path
from app.logging.logging_config import notification_logger
from app.logging.logging_decorator import log_function_call
from app.db_functions.db_schema2 import get_db, Configuration


# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# DATA STRUCTURES - DYNAMIC FIELD SUPPORT
# =============================================================================
@log_function_call(notification_logger)
def extract_infrastructure( trigger_name: str) -> Optional[str]:
        """
        Extract infrastructure from trigger name.
        
        Examples:
            "CITRIX PVS Service up" → "CITRIX"
            "OI-IBS Memory Alert" → "OI_IBS"
            "Unknown Alert" → None
        """
        if not trigger_name:
            return None
        
        trigger_upper = trigger_name.upper().strip()
        
        # Infrastructure patterns (order matters - specific first)
        patterns = [
            (r'^OI[-\s]?IBS', 'OI-IBS'),
            (r'^OI[-\s]?RDA', 'OI-RDA'),
            (r'^OI[-\s]?BA', 'OI-BA'),
            (r'^OI[-\s]?TC', 'OI-TC'),
            (r'^CITRIX', 'CITRIX'),
            (r'^DKSGD', 'DKSGD'),
            (r'^ITVIC', 'ITVIC'),
            (r'^TRIGONOVA', 'TRIGONOVA'),
            (r'^ACC', 'ACC'),
        ]
        
        for pattern, infra in patterns:
            if re.search(pattern, trigger_upper):
                logger.info(f"📍 Extracted: {infra} from '{trigger_name[:50]}'")
                if infra in ("CITRIX","TRIGONOVA"):
                    infra=infra.title()
                else:
                    infra=infra.replace("_", "-")
                return infra.strip()+" Infrastructure" if infra.strip() in ["OI-RDA","OI-IBS","Citrix","OI-BA","OI-TC","DKSGD","Trigonova","ITVIC"] else infra.strip()+"Technical"
        
        logger.warning(f"⚠️ No infrastructure found in: '{trigger_name[:50]}'")
        return None
@dataclass
class EmailData:
    """
    Email data structure from Kortex model with dynamic field support
    Core required fields + flexible additional fields
    """
    subject: str
    sender: str
    body: str
    priority: str  # P1, P2, Informational, NA - comes from Kortex
    trigger_name: str
    timestamp: Optional[str] = None  # Optional - actual email received time
    
    # Dynamic fields container - stores any additional fields from JSON
    additional_fields: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    @log_function_call(notification_logger)
    def from_dict(cls, data: Dict[str, Any]) -> 'EmailData':
        """
        Create EmailData from dictionary, automatically handling additional fields
        This allows the JSON to have any number of extra fields without code changes
        """
        # Get expected field names from the dataclass
        expected_fields = {f.name for f in fields(cls) if f.name != 'additional_fields'}
        
        # Separate known fields from additional fields
        known_fields = {}
        additional = {}
        
        for key, value in data.items():
            if key in expected_fields:
                known_fields[key] = value
            else:
                additional[key] = value
        
        # Create instance with known fields and store additional ones
        instance = cls(**known_fields)
        instance.additional_fields = additional
        
        if additional:
            logger.info(f"📝 Detected additional fields: {list(additional.keys())}")
        
        return instance
    
    @log_function_call(notification_logger)
    def to_dict(self) -> Dict[str, Any]:
        """Convert EmailData back to dictionary including all additional fields"""
        result = {
            'subject': self.subject,
            'sender': self.sender,
            'body': self.body,
            'priority': self.priority,
            'trigger_name': self.trigger_name,
            'timestamp': self.timestamp
        }
        # Add all additional fields
        result.update(self.additional_fields)
        return result
    
    @log_function_call(notification_logger)
    def get_field(self, field_name: str, default=None) -> Any:
        """
        Get any field value (core or additional) by name
        Useful for accessing dynamic fields in Jira ticket descriptions
        """
        # Check core fields first
        if hasattr(self, field_name):
            return getattr(self, field_name)
        # Check additional fields
        return self.additional_fields.get(field_name, default)


# =============================================================================
# JIRA INTEGRATION - ENHANCED FOR DYNAMIC FIELDS
# =============================================================================

class JiraIntegration:
    """Handles Jira ticket creation with dynamic field support"""
    
    def __init__(self):
        self.jira_client = None
        self.executor = ThreadPoolExecutor(max_workers=5)
        self._initialize_client()
    
    @log_function_call(notification_logger)
    def _initialize_client(self):
        """Initialize JIRA client"""
        try:
            db = next(get_db())
            latest_config = (
            db.query(Configuration)
            .order_by(Configuration.created_at.desc())
            .first())
            db.close()
            JIRA_BASE_URL = latest_config.jira_base_url
            JIRA_API_TOKEN = latest_config.jira_api_token

            self.jira_client = JIRA(
                server=JIRA_BASE_URL,
                basic_auth=(settings.JIRA_EMAIL, JIRA_API_TOKEN)
            )

            # self.jira_client = JIRA(
            #     server=settings.JIRA_BASE_URL,
            #     basic_auth=(settings.JIRA_EMAIL, settings.JIRA_API_TOKEN)
            # )
            logger.info("✅ Jira client initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Jira: {e}")
            raise
    
    @log_function_call(notification_logger)
    def _extract_machine_name(self, text: str) -> str:
        """Extract machine name from text"""
        # Pattern like DESDN01057, DEROT04428
        machine_pattern = r'(DE[A-Z]{2,4}\d{5,6})'
        match = re.search(machine_pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Pattern like .bitzer.biz
        bitzer_pattern = r'(\w+\.bitzer\.biz)'
        match = re.search(bitzer_pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
        
        return "Unknown"
    
    @log_function_call(notification_logger)
    def _get_category(self, subject: str, body: str) -> str:
        """Determine category from email content"""
        combined = f"{subject} {body}".lower()
        
        if "citrix" in combined or "pvs" in combined:
            return "CITRIX"
        elif "sap" in combined:
            return "SAP"
        elif "hypervisor" in combined or "vmware" in combined:
            return "Hypervisor/VMware"
        elif "bitzer" in combined:
            return "BITZER"
        elif "controlup" in combined:
            return "ControlUp"
        else:
            return "General"
    
    @log_function_call(notification_logger)
    def _convert_priority_to_jira(self, priority: str) -> str:
        """Convert priority to Jira priority names"""
        mapping = {
            "P1": "Highest",
            "P2": "High",
            "P3":"Medium",
            "Informational": "Low",
            "NA": "Lowest"
        }
        return mapping.get(priority, "Medium")
    
    @log_function_call(notification_logger)
    def _parse_body_field(self, body: str, field_name: str) -> str:
        """
        Parse a field value from the email body text
        Looks for patterns like "Field name: value" in the body
        """
        # Try exact match first
        pattern = rf"{field_name}:\s*(.+?)(?:\r?\n|$)"
        match = re.search(pattern, body, re.IGNORECASE | re.MULTILINE)
        if match:
            return match.group(1).strip()
        return "N/A"
    
    @log_function_call(notification_logger)
    def _build_description(self, email_data: EmailData, machine_name: str, category: str) -> str:
        """
        Build Jira ticket description by dynamically extracting all fields from email body
        Each email type may have different fields - we extract whatever is present
        """
        body = email_data.body
        
        # Start with Organization Name (always present)
        description = "Organization Name: Bitzer\n\n"
        
        # Pattern to match all "Field name: value" pairs in the body
        # Matches lines like "     Folder name: value" or "     Process ID: 1234"
        # Field name can contain letters, spaces, parentheses, and other characters
        field_pattern = r'^\s*([A-Za-z][\w\s\(\)\+\-\.\/]+?):\s*(.+?)(?:\s*<controlup://[^>]+>)?\s*$'
        
        # Pattern for special lines that don't follow "field: value" format
        # Example: "Value changed from 1 to 2.1"
        value_changed_pattern = r'^\s*(Value changed from .+?)$'
        
        # Extract all fields from body
        lines = body.split('\r\n')
        extracted_fields = {}
        
        for line in lines:
            # Try standard field:value pattern first
            match = re.match(field_pattern, line, re.IGNORECASE)
            if match:
                field_name = match.group(1).strip()
                field_value = match.group(2).strip()
                
                # Skip fields with empty values (like "Columns involved in this incident:")
                if not field_value or field_value.isspace():
                    continue
                
                # Skip the first "Organization Name" since we already added it
                # Skip generic headers that aren't actual fields
                skip_fields = [
                    'Organization Name',
                    'In order to configure',
                    'This is an automated',
                    'The monitored resource',
                    'A process was terminated'
                ]
                
                if not any(skip in field_name for skip in skip_fields):
                    extracted_fields[field_name] = field_value
            else:
                # Try special patterns for non-standard lines
                value_match = re.match(value_changed_pattern, line, re.IGNORECASE)
                if value_match:
                    # Add as a special field
                    extracted_fields["Value changed"] = value_match.group(1).strip().replace("Value changed from ", "")
        
        # Add all extracted fields to description in order
        for field_name, field_value in extracted_fields.items():
            # Special formatting for "Value changed" field
            if field_name == "Value changed":
                description += f"Value changed from {field_value}\n\n"
            else:
                description += f"{field_name}: {field_value}\n\n"
        
        return description.rstrip() + "\n"
    
    @log_function_call(notification_logger)
    def _find_attachment_file(self, machine_name: str, folder: str = "original emails") -> Optional[str]:
        """
        Find .msg file matching machine name
        Searches in multiple possible locations
        """
        from pathlib import Path
        
        if not machine_name or machine_name == "Unknown":
            logger.debug(f"   📎 No machine name to search for attachment")
            return None
        
        # List of folders to search in order of preference
        search_folders = [
            folder,  # Primary folder (default: "original emails")
            "original emails",  # With space (your actual folder)
            "original_emails",  # Alternative with underscore
            "attachments",  # Alternative folder name
            "emails",  # Another common name
            ".",  # Current directory
        ]
        
        logger.info(f"   📎 Searching for attachment matching: {machine_name}")
        
        for search_folder in search_folders:
            folder_path = Path(search_folder)
            
            if not folder_path.exists():
                logger.debug(f"      Folder not found: {search_folder}")
                continue
            
            try:
                # Search for .msg files
                msg_files = list(folder_path.glob("*.msg"))
                logger.debug(f"      Found {len(msg_files)} .msg files in '{search_folder}'")
                
                for msg_file in sorted(msg_files):
                    if machine_name.upper() in msg_file.name.upper():
                        logger.info(f"   ✅ Found attachment: {msg_file.name} in '{search_folder}/'")
                        return str(msg_file)
                        
            except Exception as e:
                logger.debug(f"      Error searching {search_folder}: {e}")
        
        logger.warning(f"   ⚠️  No attachment found for machine: {machine_name}")
        logger.info(f"      Searched folders: {', '.join([repr(f) for f in search_folders])}")
        return None




    @log_function_call(notification_logger)
    def _attach_original_email(self, issue_key: str, attachment_path: str) -> bool:
        try:
            if not attachment_path:
                return True

            attachment_file = Path(attachment_path)

            if not attachment_file.exists():
                print(f"⚠️ File not found: {attachment_path}")
                return True 

            # CRITICAL FIX 1: Check for 0-byte files (often caused by Outlook locks)
            file_size = attachment_file.stat().st_size
            if file_size == 0:
                print(f"⚠️ File is empty (0 bytes). It might be locked by Outlook.")
                return True

            # CRITICAL FIX 2: Sanitize the filename for the Jira API
            # Jira Cloud hates long filenames with spaces in the API header
            clean_name = re.sub(r'[^a-zA-Z0-9._-]', '_', attachment_file.stem) # Clean the name part
            clean_filename = f"{clean_name[:50]}{attachment_file.suffix}"     # Limit to 50 chars + extension

            print(f"📎 Uploading as: {clean_filename} ({file_size/1024:.1f} KB)")

            # CRITICAL FIX 3: Ensure read binary mode and cursor is at start
            with open(attachment_file, 'rb') as f:
                # Just in case the file was read previously, reset cursor to 0
                f.seek(0) 
                
                self.jira_client.add_attachment(
                    issue=issue_key,
                    attachment=f,
                    filename=clean_filename  # We send the CLEAN name, not the local file name
                )

            print(f"✅ Attachment successfully added to {issue_key}")
            return True

        except Exception as e:
            print(f"❌ Failed to attach file: {e}")
            return True
    from jira import JIRA

    @log_function_call(notification_logger)
    def assign_issue_by_email(self,issue_key,email_address):
        try:
            # Step A: Search for the user to get their Account ID
            # We limit results to 1 since emails are unique
            users = self.jira_client.search_users(query=email_address, maxResults=1)
            
            if not users:
                print(f"User with email {email_address} not found.")
                return

            user = users[0]
            account_id = user.accountId
            
            issue = self.jira_client.issue(issue_key)
            issue.update(assignee={'accountId': account_id})
            
            print(f"Successfully assigned {issue_key} to {user.displayName} ({account_id})")

        except Exception as e:
            print(f"Error: {e}")

    @log_function_call(notification_logger)
    def create_ticket_sync(self, email_data: Dict,email) -> Optional[Tuple[str, str, str, str]]:
        """Create Jira ticket (sync operation for executor) - Returns ticket key, machine name, category, and assignee"""
        try:
            #machine_name = self._extract_machine_name(f"{email_data.subject} {email_data.body}")
            machine_name =email_data["resource_name"]
            category = self._get_category(email_data["subject"], email_data["body"])
            #category = email_data[""]
            # Build ticket summary - format: "Trigger name - Machine name"
            summary = f"{email_data['trigger_name']} - {machine_name}"
            
            #description=email_data['body']+email_data['generated_summary']
            description=email_data['generated_summary']
            # Create issue
            
            issue = self.jira_client.create_issue(
                project=settings.JIRA_PROJECT_KEY,
                summary=summary,
                description=description,
                issuetype={'name': "[System] Incident"},
                priority={'name':self._convert_priority_to_jira(email_data['priority'])}
            )
            
            # Get the assignee name - default to "Team" if no assignee
            self.assign_issue_by_email(issue_key=issue.key,email_address=email)
            assignee_name = email
            if issue.fields.assignee:
                assignee_name = issue.fields.assignee.displayName
            
            
            logger.info(f"   ✅ Jira ticket created: {issue.key} (Assignee: {assignee_name})")
            self._attach_original_email(issue.key,email_data["path"])
            return issue.key, machine_name, category, assignee_name
            
        except Exception as e:
            logger.error(f"   ❌ Failed to create Jira ticket: {e}")
            return None
    @log_function_call(notification_logger)
    def _create_attachment(self,email_subject,email_body,ISSUE_KEY):
        msg = EmailMessage()
        sender_email = "ControlUp@bitzer.de"
        recipient_email = "Monitoring.AI@bitzer.de"
        msg['Subject'] = email_subject
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg.set_content(email_body)
        eml_content = msg.as_bytes()
        
        try:
            eml_file = io.BytesIO(eml_content)
            filename = f"C:\\temp\\{email_subject}.eml"
            print(f"Attaching '{filename}' to issue {ISSUE_KEY}...")
            
            # The 'attachment' argument requires a file-like object
            self.jira_client.add_attachment(issue=ISSUE_KEY, attachment=eml_file, filename=filename)
    
            print(f"Successfully attached {filename} to {ISSUE_KEY}.")

        except Exception as e:
            print(f"An error occurred: {e}")
    @log_function_call(notification_logger)
    async def create_ticket(self, email_data: EmailData) -> Optional[Tuple[str, str, str, str]]:
        """
        Create Jira ticket asynchronously
        Returns tuple of (ticket_key, machine_name, category, assignee_name)
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self._create_ticket_sync,
            email_data
        )
    @log_function_call(notification_logger)
    async def close(self):
        """Clean up resources"""
        self.executor.shutdown(wait=False)

