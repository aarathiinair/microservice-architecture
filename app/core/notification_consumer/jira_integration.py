import logging
import asyncio
import aiohttp
import re
import hashlib
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
# Added DuplicateEmail to imports
from app.db_functions.db_schema2 import get_db, Configuration, SegregatedEmail, JiraEntry, DuplicateEmail
from sqlalchemy.orm import Session
from sqlalchemy import desc


# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

TEAM_FIELD_ID = "customfield_10001"

# Team Name (from trigger_mappings.team) → Jira Team UUID
TEAM_UUID_MAP = {
    # IBS Teams
    "IBS - CITRIX": "8b916750-c421-46b3-b56b-840b84a721c1",  # Missing in Jira 
    "IBS - Virtual Server Infrastructure": "be18814d-a872-432f-9d48-aa8a41b61b80",
    "IBS - Mail Service": "og-82d9c204-17c0-46fb-a396-b412a2eb857e",
    "IBS - Backup":  "bcc61bda-6d78-4566-aaf6-c236d8703a81",  # Missing in Jira
    "IBS - ROT": "eda8c020-1ee2-490b-bde6-baa2ef36269d",
    
    # SAP Teams
    "SAP Basis": "cbc86a6e-8c12-4e3a-8ecd-d4c52b83b17b",
    "SAP Sales": "4c652e69-e207-4e98-b4bf-ca90838de87b",
    "SAP Operations": "c066a998-37cd-4f7e-ac31-f35fd8543910",
    "SAP Development": "ac2f0447-b1f2-4d7e-bc3e-bf7e9bf377d6",
    
    # OI Teams
    "OI - DB Development": "e2435921-b8cd-4685-8554-83bd8023a198",  
    "OI - DB Administration": "b2ebb2ae-c227-41ae-ac40-5a670d52bc87",  # Missing in Jira
    "OI - IBS": "54292b37-54d3-4e43-a406-4732afbfad4d",
    "OI - RDA": "8c63b9c0-21ea-4cb3-b925-f113cc0c31eb",
    "OI - Telecommunications": "og-d9b1de6e-6a08-4039-b1a4-9cb31b025608",
    
    # Fallback
    #"General": None,
}

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
# JIRA INTEGRATION - ENHANCED WITH DUPLICATE CHECKING
# =============================================================================

class JiraIntegration:
    """Handles Jira ticket creation with dynamic field support and duplicate checking"""
    
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
            logger.info("✅ Jira client initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Jira: {e}")
            raise

    # =========================================================================
    # NEW METHODS FOR DUPLICATE CHECKING
    # =========================================================================

    @log_function_call(notification_logger)
    def get_ticket_status(self, jira_ticket_id: str) -> Optional[str]:
        """
        Get the current status of a Jira ticket by calling Jira API.
        
        Args:
            jira_ticket_id: The Jira ticket key (e.g., 'MAI-1234')
        
        Returns:
            Status string like 'Open', 'In Progress', 'Resolved', 'Closed', 'Done'
            Returns None if ticket not found or error occurs
        
        Example:
            status = self.get_ticket_status("MAI-1234")
            # Returns: "Open" or "Closed" or "In Progress" etc.
        """
        try:
            # Call Jira API to get the ticket details
            # self.jira_client was initialized in __init__
            issue = self.jira_client.issue(jira_ticket_id)
            
            # Extract the status name from the response
            # issue.fields contains all ticket fields
            # issue.fields.status is the status object
            # issue.fields.status.name is the human-readable status like "Open"
            status = issue.fields.status.name
            
            logger.info(f"📋 Ticket {jira_ticket_id} status: {status}")
            return status
            
        except Exception as e:
            # This could happen if:
            # - Ticket was deleted
            # - Network error
            # - Invalid ticket ID
            # - Permission issues
            logger.error(f"❌ Could not get status for {jira_ticket_id}: {e}")
            return None

    @log_function_call(notification_logger)
    def is_ticket_open(self, jira_ticket_id: str) -> bool:
        """
        Check if a Jira ticket is still open (not resolved/closed).
        
        This is a helper function that:
        1. Calls get_ticket_status() to get the status text
        2. Compares it against a list of "open" statuses
        3. Returns True if open, False if closed
        
        Args:
            jira_ticket_id: The Jira ticket key (e.g., 'MAI-1234')
        
        Returns:
            True  → Ticket is open/in progress (DON'T create new ticket)
            False → Ticket is closed/resolved (CREATE new ticket)
                    Also returns False if we can't determine status (safe default)
        
        Example:
            if self.is_ticket_open("MAI-1234"):
                print("Ticket is open, skip creating new one")
            else:
                print("Ticket is closed, create new one")
        """
        # Step 1: Get the status text from Jira
        status = self.get_ticket_status(jira_ticket_id)
        
        # Step 2: If we couldn't get the status, assume ticket is closed
        # This is the safe default - we'd rather create a new ticket than miss an issue
        if status is None:
            logger.warning(f"⚠️ Could not determine status for {jira_ticket_id}, treating as closed")
            return False
        
        # Step 3: Define which statuses mean "ticket is still open"
        # These are common Jira status names - adjust based on your Jira setup
        open_statuses = [
            'open',           # Default open status
            'in progress',    # Someone is working on it
            'to do',          # In backlog but not closed
            'new',            # Just created
            'reopened',       # Was closed but reopened
            'pending',        # Waiting for something
            'waiting',        # Waiting for response
            'in review',      # Being reviewed
        ]
        
        # Step 4: Check if current status is in the open list
        # .lower() converts "Open" to "open" for case-insensitive comparison
        is_open = status.lower() in open_statuses
        
        logger.info(f"📊 Ticket {jira_ticket_id} is {'OPEN' if is_open else 'CLOSED'}")
        return is_open

    @log_function_call(notification_logger)
    def find_previous_jira_ticket(self, trigger_name: str, resource_name: str, 
                                   current_email_id: str) -> Optional[str]:
        """
        Find the most recent Jira ticket for the same trigger+resource combination.
        
        This uses a JOIN query to efficiently find the most recent email that:
        1. Has the same trigger_name
        2. Has the same resource_name  
        3. Has a Jira ticket created for it
        4. Is NOT the current email
        
        Args:
            trigger_name: The trigger name from current email (e.g., "CITRIX PVS Service down")
            resource_name: The machine/resource name (e.g., "DEROT001")
            current_email_id: The email_id of the current email (to exclude it from search)
        
        Returns:
            jira_ticket_id (e.g., "MAI-1234") if found
            None if no previous ticket exists
        
        Example:
            ticket_id = self.find_previous_jira_ticket(
                trigger_name="CITRIX Alert",
                resource_name="DEROT001", 
                current_email_id="abc123hash"
            )
            # Returns: "MAI-1234" or None
        """
        db: Session = None
        try:
            # Get database session
            db = next(get_db())
            
            # =====================================================================
            # QUERY EXPLANATION:
            # 
            # We're joining two tables:
            # - segregated_email: Contains trigger_name, resource_name, email_id
            # - jira_table: Contains email_id, jiraticket_id
            #
            # The JOIN connects them on email_id
            #
            # Filters:
            # - Same trigger_name as current email
            # - Same resource_name as current email
            # - NOT the current email itself
            # - Has a Jira ticket (jiraticket_id is not None)
            #
            # Order by inserted_at DESC to get the MOST RECENT one first
            # LIMIT 1 to get only the most recent
            # =====================================================================
            
            result = db.query(
                SegregatedEmail.email_id,       # Get the email_id
                JiraEntry.jiraticket_id         # Get the Jira ticket ID
            ).join(
                # JOIN segregated_email with jira_table on email_id
                JiraEntry, 
                SegregatedEmail.email_id == JiraEntry.email_id
            ).filter(
                # Filter 1: Same trigger name
                SegregatedEmail.trigger_name == trigger_name,
                # Filter 2: Same resource/machine name
                SegregatedEmail.resource_name == resource_name,
                # Filter 3: Exclude current email
                SegregatedEmail.email_id != current_email_id,
                # Filter 4: Must have a Jira ticket
                JiraEntry.jiraticket_id.isnot(None)
            ).order_by(
                # Order by newest first
                desc(SegregatedEmail.inserted_at)
            ).first()  # Get only the first (most recent) result
            
            # =====================================================================
            # RESULT HANDLING:
            # 
            # result is either:
            # - A tuple like ('email123', 'MAI-1234') if found
            # - None if no matching record exists
            # =====================================================================
            
            if result:
                email_id, jira_ticket_id = result
                logger.info(f"🔍 Found previous ticket: {jira_ticket_id} for trigger='{trigger_name[:30]}', resource='{resource_name}'")
                return jira_ticket_id
            else:
                logger.info(f"🆕 No previous ticket found for trigger='{trigger_name[:30]}', resource='{resource_name}'")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error finding previous Jira ticket: {e}")
            # On error, return None (will create new ticket - safe default)
            return None
            
        finally:
            # Always close the database session
            if db:
                db.close()

    @log_function_call(notification_logger)
    def should_create_ticket(self, trigger_name: str, resource_name: str, 
                              current_email_id: str) -> Tuple[bool, Optional[str]]:
        """
        Main decision function: Should we create a new Jira ticket for this email?
        
        This is the main entry point for duplicate checking. It:
        1. Searches for any previous Jira ticket with same trigger+resource
        2. If found, checks if that ticket is still open
        3. Returns decision: create or skip
        
        Args:
            trigger_name: The trigger name from current email
            resource_name: The machine/resource name
            current_email_id: The email_id of current email
        
        Returns:
            Tuple of (should_create: bool, reason: str)
            
            (True, None) → Create new ticket (no previous ticket OR previous is closed)
            (False, "MAI-1234") → Skip (previous ticket MAI-1234 is still open)
        
        Example:
            should_create, existing_ticket = self.should_create_ticket(
                trigger_name="CITRIX Alert",
                resource_name="DEROT001",
                current_email_id="abc123"
            )
            
            if should_create:
                # Create new Jira ticket
                pass
            else:
                print(f"Skipping - ticket {existing_ticket} is still open")
        """
        logger.info(f"🔎 Checking if ticket should be created for trigger='{trigger_name[:40]}', resource='{resource_name}'")
        
        # =====================================================================
        # STEP 1: Find the most recent Jira ticket for same trigger+resource
        # =====================================================================
        previous_ticket_id = self.find_previous_jira_ticket(
            trigger_name=trigger_name,
            resource_name=resource_name,
            current_email_id=current_email_id
        )
        
        # =====================================================================
        # STEP 2: If no previous ticket found, create new one
        # =====================================================================
        if previous_ticket_id is None:
            logger.info(f"✅ No previous ticket found - WILL CREATE new ticket")
            return (True, None)
        
        # =====================================================================
        # STEP 3: Previous ticket exists - check if it's still open
        # =====================================================================
        if self.is_ticket_open(previous_ticket_id):
            # Ticket is OPEN → Don't create new ticket
            logger.info(f"⏭️ Previous ticket {previous_ticket_id} is OPEN - SKIPPING ticket creation")
            return (False, previous_ticket_id)
        else:
            # Ticket is CLOSED → Create new ticket
            logger.info(f"✅ Previous ticket {previous_ticket_id} is CLOSED - WILL CREATE new ticket")
            return (True, None)

    # =========================================================================
    # EXISTING METHODS (unchanged)
    # =========================================================================

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
        field_pattern = r'^\s*([A-Za-z][\w\s\(\)\+\-\.\/]+?):\s*(.+?)(?:\s*<controlup://[^>]+>)?\s*$'
        
        # Pattern for special lines that don't follow "field: value" format
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
                
                # Skip fields with empty values
                if not field_value or field_value.isspace():
                    continue
                
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
                    extracted_fields["Value changed"] = value_match.group(1).strip().replace("Value changed from ", "")
        
        # Add all extracted fields to description in order
        for field_name, field_value in extracted_fields.items():
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
        if not machine_name or machine_name == "Unknown":
            logger.debug(f"   📎 No machine name to search for attachment")
            return None
        
        # List of folders to search in order of preference
        search_folders = [
            folder,
            "original emails",
            "original_emails",
            "attachments",
            "emails",
            ".",
        ]
        
        logger.info(f"   📎 Searching for attachment matching: {machine_name}")
        
        for search_folder in search_folders:
            folder_path = Path(search_folder)
            
            if not folder_path.exists():
                logger.debug(f"      Folder not found: {search_folder}")
                continue
            
            try:
                msg_files = list(folder_path.glob("*.msg"))
                logger.debug(f"      Found {len(msg_files)} .msg files in '{search_folder}'")
                
                for msg_file in sorted(msg_files):
                    if machine_name.upper() in msg_file.name.upper():
                        logger.info(f"   ✅ Found attachment: {msg_file.name} in '{search_folder}/'")
                        return str(msg_file)
                        
            except Exception as e:
                logger.debug(f"      Error searching {search_folder}: {e}")
        
        logger.warning(f"   ⚠️  No attachment found for machine: {machine_name}")
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

            file_size = attachment_file.stat().st_size
            if file_size == 0:
                print(f"⚠️ File is empty (0 bytes). It might be locked by Outlook.")
                return True

            clean_name = re.sub(r'[^a-zA-Z0-9._-]', '_', attachment_file.stem)
            clean_filename = f"{clean_name[:50]}{attachment_file.suffix}"

            print(f"📎 Uploading as: {clean_filename} ({file_size/1024:.1f} KB)")

            with open(attachment_file, 'rb') as f:
                f.seek(0) 
                
                self.jira_client.add_attachment(
                    issue=issue_key,
                    attachment=f,
                    filename=clean_filename
                )

            print(f"✅ Attachment successfully added to {issue_key}")
            return True

        except Exception as e:
            print(f"❌ Failed to attach file: {e}")
            return True

    @log_function_call(notification_logger)
    def assign_issue_by_email(self, issue_key, email_address):
        try:
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
    def set_team_field(self, issue_key: str, team_name: str) -> bool:
        """
        Set Team field on Jira issue using team name from trigger_mappings.
        """
        if not team_name:
            logger.warning(f"⚠️ No team name provided for {issue_key}")
            return False
        
        team_id = TEAM_UUID_MAP.get(team_name)
        
        if not team_id:
            logger.warning(f"⚠️ Team '{team_name}' not found in UUID map, skipping team assignment for {issue_key}")
            return False
        
        try:
            issue = self.jira_client.issue(issue_key)
            issue.update(fields={TEAM_FIELD_ID: team_id})
            logger.info(f"✅ Set Team to '{team_name}' for {issue_key}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to set Team for {issue_key}: {e}")
            return False

    @log_function_call(notification_logger)
    def create_ticket_sync(self, email_data: Dict, email: str) -> Optional[Tuple[str, str, str, str]]:
        """
        Create Jira ticket (sync operation for executor).
        
        NOW INCLUDES DUPLICATE CHECKING:
        Before creating a ticket, checks if a previous open ticket exists
        for the same trigger+resource combination.
        
        Returns:
            Tuple of (ticket_key, machine_name, category, assignee_name) if ticket created
            None if ticket creation skipped (duplicate with open ticket)
            None if ticket creation failed
        """
        try:
            # =================================================================
            # STEP 1: Extract basic info from email
            # =================================================================
            machine_name = email_data.get("resource_name", "Unknown")
            trigger_name = email_data.get("trigger_name", "")
            email_id = email_data.get("email_id", "")
            
            category = self._get_category(email_data.get("subject", ""), email_data.get("body", ""))
            
            # =================================================================
            # STEP 2: DUPLICATE CHECK - Should we create a ticket?
            # =================================================================
            should_create, existing_ticket = self.should_create_ticket(
                trigger_name=trigger_name,
                resource_name=machine_name,
                current_email_id=email_id
            )
            
            if not should_create:
                # Previous ticket is still OPEN - don't create new ticket
                logger.info(f"⏭️ SKIPPING ticket creation - existing open ticket: {existing_ticket}")
                print(f"⏭️ SKIPPING ticket creation for {machine_name} - existing open ticket: {existing_ticket}")
                
                # --- NEW LOGIC: POPULATE DUPLICATE EMAILS TABLE ---
                db = None
                try:
                    db = next(get_db())
                    
                    # Generate hash for duplicate_email_id
                    # Using sha256 of email_id + current time to ensure uniqueness of the duplicate record
                    unique_string = f"{email_id}_{datetime.now().isoformat()}"
                    duplicate_id = hashlib.sha256(unique_string.encode()).hexdigest()
                    
                    # Handle timestamp - default to now(), parse if available
                    received_time = datetime.now()
                    ts_val = email_data.get('timestamp')
                    if ts_val:
                        try:
                            # Attempt to parse timestamp if it's a string, assuming ISO format
                            if isinstance(ts_val, str):
                                received_time = datetime.fromisoformat(ts_val)
                            elif isinstance(ts_val, datetime):
                                received_time = ts_val
                        except Exception:
                            # If parsing fails, stick with datetime.now()
                            pass

                    duplicate_entry = DuplicateEmail(
                        email_id=email_id,
                        duplicate_email_id=duplicate_id,
                        subject=email_data.get("subject"),
                        body=email_data.get("body"),
                        sender=email_data.get("sender"),
                        received_at=received_time
                        # inserted_at is handled by schema default
                    )
                    
                    db.add(duplicate_entry)
                    db.commit()
                    logger.info(f"📝 Duplicate email recorded in DB: {duplicate_id}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to record duplicate email: {e}")
                finally:
                    if db:
                        db.close()
                # --------------------------------------------------

                # Return None to indicate no ticket was created
                return None, None, None, None
            
            # =================================================================
            # STEP 3: Create the Jira ticket (existing logic)
            # =================================================================
            logger.info(f"🎫 Creating new Jira ticket for {trigger_name[:40]} - {machine_name}")
            
            # Build ticket summary - format: "Trigger name - Machine name"
            summary = f"{trigger_name} - {machine_name}"
            
            description = email_data.get('generated_summary', '')
            
            # Create issue
            issue = self.jira_client.create_issue(
                project=settings.JIRA_PROJECT_KEY,
                summary=summary,
                description=description,
                issuetype={'name': "[System] Incident"},
                priority={'name': self._convert_priority_to_jira(email_data.get('priority', 'Medium'))}
            )
            
            # Assign the issue
            self.assign_issue_by_email(issue_key=issue.key, email_address=email)
            assignee_name = email
            if issue.fields.assignee:
                assignee_name = issue.fields.assignee.displayName
            
            logger.info(f"   ✅ Jira ticket created: {issue.key} (Assignee: {assignee_name})")
            
            # Attach original email
            self._attach_original_email(issue.key, email_data.get("path", ""))
            
            return issue.key, machine_name, category, assignee_name
            
        except Exception as e:
            logger.error(f"   ❌ Failed to create Jira ticket: {e}")
            return None

    @log_function_call(notification_logger)
    def _create_attachment(self, email_subject, email_body, ISSUE_KEY):
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
            email_data.to_dict(), # Convert to dict because sync version expects dict
            "Monitoring.AI@bitzer.de" # Default email
        )

    @log_function_call(notification_logger)
    async def close(self):
        """Clean up resources"""
        self.executor.shutdown(wait=False)