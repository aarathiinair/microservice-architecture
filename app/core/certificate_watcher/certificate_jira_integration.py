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

class CertificateJiraIntegration:
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
    def create_ticket_sync(self, certificate_details) -> Optional[Tuple[str, str, str, str]]:
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
            certificate_name=certificate_details.get("certificate_name",None)
            description=certificate_details.get("description",None)
            #usage=certificate_details.get("usage",None)
            status=certificate_details.get("status",None) 
            responsible_group=certificate_details.get("responsible_group",None)
            team_channel=certificate_details.get("team_channel",None)
            expiration_timestamp=certificate_details.get("expiration_timestamp",None)

            
            summary= f"Certificate : {certificate_name}" + f" Used by {team_channel}" + f" is {status}" +f" on {expiration_timestamp}" 
            
            # Create issue
            issue = self.jira_client.create_issue(
                project=settings.JIRA_PROJECT_KEY,
                summary=summary,
                description=description,
                issuetype={'name': "[System] Incident"},
                priority={'name': self._convert_priority_to_jira("P1")}
            )
            
            # Assign the issue
            #self.set_team_field(issue_key=issue.key, team_name=responsible_group)
            self.assign_issue_by_email(issue_key=issue.key,email_address="daniel.buschmann@bitzer.de")
            logger.info(f"   ✅ Jira ticket created: {issue.key} (Assignee: {responsible_group})")
            
            
            
            return issue.key
            
        except Exception as e:
            logger.error(f"   ❌ Failed to create Jira ticket: {e}")
            return None


    @log_function_call(notification_logger)
    async def close(self):
        """Clean up resources"""
        self.executor.shutdown(wait=False)