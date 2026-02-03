#!/usr/bin/env python3
"""
Teams Integration with Trigger-Based Channel Routing

Routes notifications to Teams channels based on trigger name matching
against the trigger_mappings database table (loaded from Excel spreadsheet).
"""
import os
from datetime import datetime
import asyncio
import logging
import json
import re
import httpx
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

from app.config import settings
from app.db_functions.db_schema2 import get_db, Server, TriggerMapping
from sqlalchemy.orm import Session
from app.logging.logging_config import notification_logger
from app.logging.logging_decorator import log_function_call


# Setup logging - reduce noise from httpx
logging.getLogger("httpx").setLevel(logging.WARNING)

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class EmailData:
    """Email data structure"""
    subject: str
    sender: str
    body: str
    priority: str
    trigger_name: str
    timestamp: str
    resource_name: Optional[str] = None
    
    @classmethod
    @log_function_call(notification_logger)
    def from_dict(cls, data: dict) -> 'EmailData':
        return cls(
            subject=data.get('subject', ''),
            sender=data.get('sender', ''),
            body=data.get('body', ''),
            priority=data.get('priority', 'Unknown'),
            trigger_name=data.get('trigger_name', ''),
            timestamp=data.get('timestamp', ''),
            resource_name=data.get('resource_name')
        )


# ==============================================================================
# TriggerMatcher: Fuzzy matching for trigger-based channel routing
# ==============================================================================
class TriggerMatcher:
    """
    Matches incoming trigger names to database trigger mappings using fuzzy matching.
    Uses combination of SequenceMatcher and token-based Jaccard similarity.
    """
    
    MATCH_THRESHOLD = 0.75  # 75% similarity required for a match
    
    def __init__(self):
        self.trigger_mappings: List[Dict] = []
        self.match_cache: Dict[str, Tuple[str, float, str]] = {}
        self._load_mappings()
    
    @log_function_call(notification_logger)
    def _load_mappings(self):
        """Load trigger mappings from database."""
        try:
            db: Session = next(get_db())
            try:
                mappings = db.query(TriggerMapping).all()
                self.trigger_mappings = [
                    {
                        'trigger_name': m.trigger_name,
                        'team': m.team,
                        'category': m.category,
                        'priority': m.priority,
                        'normalized': self._normalize_text(m.trigger_name)
                    }
                    for m in mappings
                ]
                logger.info(f"✅ Loaded {len(self.trigger_mappings)} trigger mappings from database")
            finally:
                db.close()
        except Exception as e:
            logger.warning(f"⚠️ Could not load trigger mappings: {e}")
            self.trigger_mappings = []
    
    @log_function_call(notification_logger)
    def reload_mappings(self):
        """Reload mappings from database (call after Excel import)."""
        self.match_cache.clear()
        self._load_mappings()
    
    @staticmethod
    @log_function_call(notification_logger)
    def _normalize_text(text: str) -> str:
        """Normalize text for comparison."""
        if not text:
            return ""
        # Lowercase
        text = text.lower()
        # Remove controlup:// links
        text = re.sub(r'controlup://[^\s]+', '', text)
        # Remove punctuation except spaces
        text = re.sub(r'[^\w\s]', ' ', text)
        # Collapse whitespace
        text = ' '.join(text.split())
        return text.strip()
    
    @staticmethod
    @log_function_call(notification_logger)
    def _tokenize(text: str) -> set:
        """Split normalized text into tokens."""
        return set(text.split()) if text else set()
    
    @log_function_call(notification_logger)
    def _calculate_similarity(self, input_text: str, db_text: str) -> float:
        """
        Calculate similarity between two strings using combined approach:
        - SequenceMatcher for sequence similarity
        - Jaccard similarity for token overlap
        """
        if not input_text or not db_text:
            return 0.0
        
        # Normalize both texts
        norm_input = self._normalize_text(input_text)
        norm_db = self._normalize_text(db_text)
        
        if not norm_input or not norm_db:
            return 0.0
        
        # Exact match check (fast path)
        if norm_input == norm_db:
            return 1.0
        
        # SequenceMatcher similarity
        seq_ratio = SequenceMatcher(None, norm_input, norm_db).ratio()
        
        # Token-based Jaccard similarity
        tokens_input = self._tokenize(norm_input)
        tokens_db = self._tokenize(norm_db)
        
        if tokens_input and tokens_db:
            intersection = len(tokens_input & tokens_db)
            union = len(tokens_input | tokens_db)
            jaccard = intersection / union if union > 0 else 0.0
        else:
            jaccard = 0.0
        
        # Weighted combination (favor token overlap slightly)
        combined = (seq_ratio * 0.45) + (jaccard * 0.55)
        
        return combined
    
    @log_function_call(notification_logger)
    def find_best_match(self, trigger_name: str) -> Tuple[str, float, str, Optional[str]]:
        """
        Find the best matching team for a given trigger name.
        
        Args:
            trigger_name: The trigger name from the incoming email
            
        Returns:
            Tuple of (team_name, confidence_score, matched_db_trigger, responsible_person)
            Returns ("General", 0.0, "", None) if no good match found
        """
        if not trigger_name:
            return ("General", 0.0, "", None)
        
        # Check cache first
        cache_key = self._normalize_text(trigger_name)
        if cache_key in self.match_cache:
            return self.match_cache[cache_key]
        
        if not self.trigger_mappings:
            logger.warning("No trigger mappings loaded - routing to General")
            return ("General", 0.0, "", None)
        
        best_match = None
        best_score = 0.0
        best_trigger = ""
        best_responsible_person = None
        
        for mapping in self.trigger_mappings:
            score = self._calculate_similarity(trigger_name, mapping['trigger_name'])
            
            # Early exit for very high matches
            if score >= 0.9:
                result = (mapping['team'], score, mapping['trigger_name'], mapping.get('responsible_persons'))
                self.match_cache[cache_key] = result
                return result
            
            if score > best_score:
                best_score = score
                best_match = mapping['team']
                best_trigger = mapping['trigger_name']
                best_responsible_person = mapping.get('responsible_persons')
        
        # Apply threshold
        if best_score >= self.MATCH_THRESHOLD:
            result = (best_match, best_score, best_trigger, best_responsible_person)
        else:
            result = ("General", best_score, best_trigger, None)
        
        # Cache the result
        self.match_cache[cache_key] = result
        return result





class TeamsIntegration:
    """Handles Teams notifications with trigger-based channel routing"""
    
    def __init__(self):
        self.trigger_matcher = TriggerMatcher()
        self.enabled = settings.MS_TEAMS_ENABLED
        self.unmatched_triggers_log = "logs/unmatched_triggers.txt"

    def _log_unmatched_trigger(self, trigger_name: str, incident_timestamp: str):
        
       
        
        try:
            # Ensure the logs directory exists
            os.makedirs(os.path.dirname(self.unmatched_triggers_log), exist_ok=True)
            
            # Format: Trigger: <trigger_name> | Incident Timestamp: <incident_timestamp>
            log_entry = f"Trigger: {trigger_name} | Incident Timestamp: {incident_timestamp}\n"
            
            with open(self.unmatched_triggers_log, 'a', encoding='utf-8') as f:
                f.write(log_entry)
                
            logger.info(f"   📝 Logged unmatched trigger to {self.unmatched_triggers_log}")
        except Exception as e:
            logger.error(f"   ❌ Failed to log unmatched trigger: {e}")
    
    @log_function_call(notification_logger)
    def _extract_machine_name(self, email_data: EmailData) -> Optional[str]:
        """Extract machine/resource name from email data"""
        # Try resource_name field first
        if email_data.resource_name:
            name = email_data.resource_name
            if '@' in name:
                name = name.split('@')[0]
            elif '.' in name and '.bitzer' in name.lower():
                name = name.split('.')[0]
            return name.upper()
        
        # Try to extract from subject - improved patterns
        subject = email_data.subject
        patterns = [
            r'Machine\s+([A-Za-z0-9]+)\.bitzer',  # Machine DEROT02010.bitzer
            r'Computer\s+([A-Za-z0-9]+)\.bitzer',  # Computer DESDN04199.bitzer
            r'on\s+([A-Za-z0-9]+)\s+\(',          # on DESDN01057 (
            r'([A-Z]{2}[A-Z0-9]{3,}[0-9]+)',       # Pattern like DEROT02010, DESDN04199
        ]
        
        for pattern in patterns:
            match = re.search(pattern, subject, re.IGNORECASE)
            if match:
                return match.group(1).upper()
        
        # Try body with same patterns
        for pattern in patterns:
            match = re.search(pattern, email_data.body, re.IGNORECASE)
            if match:
                return match.group(1).upper()
        
        return None
    
    @log_function_call(notification_logger)
    def _format_timestamp(self, timestamp: Optional[str]) -> str:
        """Format timestamp for Teams notification"""
        from datetime import datetime
        if not timestamp:
            return datetime.now().strftime("%Y-%m-%d %H:%M CST")
        
        try:
            dt = datetime.fromisoformat(timestamp.replace('+00:00', '+0000'))
            return dt.strftime("%Y-%m-%d %H:%M CST")
        except:
            return timestamp
    
    @log_function_call(notification_logger)
    def _extract_clean_sender(self, sender: str) -> str:
        """Extract clean email address from sender string"""
        email_pattern = r'<?([\w\.-]+@[\w\.-]+)>?'
        match = re.search(email_pattern, sender)
        if match:
            return match.group(1)
        return sender.replace('"', '').strip()
    
    @log_function_call(notification_logger)
    def _build_adaptive_card(self, email_data: EmailData, jira_key: str = None,
                             machine_name: str = None, infrastructure: str = None,
                             assignee: str = None) -> Dict:
        """Build Adaptive Card payload for Power Automate webhook"""
        
        # Clean sender email
        clean_sender = self._extract_clean_sender(email_data.sender)
        
        # Format timestamp
        formatted_timestamp = self._format_timestamp(email_data.timestamp)
        
        # Build table rows data
        table_data = [
            {"col1": "Source", "col2": clean_sender},
            {"col1": "Resource Name", "col2": machine_name or "N/A"},
            {"col1": "Trigger Name", "col2": email_data.trigger_name or "N/A"},
            {"col1": "Priority", "col2": email_data.priority},
            {"col1": "Incident Timestamp", "col2": formatted_timestamp},
        ]
        
        if infrastructure:
            table_data.append({"col1": "Infrastructure", "col2": infrastructure})
        if jira_key:
            table_data.append({"col1": "JIRA Ticket", "col2": jira_key})
        if assignee:
            table_data.append({"col1": "Assignee", "col2": assignee})
        
        # Build column sets for table
        table_rows = []
        
        # Add header row
        table_rows.append({
            "type": "ColumnSet",
            "columns": [
                {
                    "type": "Column",
                    "width": "150px",
                    "items": [{
                        "type": "TextBlock",
                        "text": "**Details**",
                        "weight": "Bolder"
                    }]
                },
                {
                    "type": "Column",
                    "width": "350px",
                    "items": [{
                        "type": "TextBlock",
                        "text": "**Value**",
                        "weight": "Bolder"
                    }]
                }
            ],
            "separator": True
        })
        
        # Add data rows
        for row in table_data:
            table_rows.append({
                "type": "ColumnSet",
                "columns": [
                    {
                        "type": "Column",
                        "width": "150px",
                        "items": [{
                            "type": "TextBlock",
                            "text": row["col1"],
                            "wrap": True
                        }]
                    },
                    {
                        "type": "Column",
                        "width": "350px",
                        "items": [{
                            "type": "TextBlock",
                            "text": row["col2"],
                            "wrap": True
                        }]
                    }
                ],
                "separator": True
            })
        
        # Determine assignee name for greeting
        greeting_name = assignee if assignee else "Team"
        
        return {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "Container",
                            "style": "emphasis",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": "MS Teams Incident Notification",
                                    "weight": "Bolder",
                                    "size": "Large"
                                }
                            ],
                            "bleed": True
                        },
                        {
                            "type": "TextBlock",
                            "text": f"**Incident Notification: {email_data.subject}**",
                            "wrap": True,
                            "spacing": "Medium",
                            "size": "Medium",
                            "weight": "Bolder"
                        },
                        {
                            "type": "TextBlock",
                            "text": f"Hi {greeting_name},",
                            "wrap": True,
                            "spacing": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": "The ControlUp monitoring system has reported an incident. Please review the details below and take appropriate action:",
                            "wrap": True,
                            "spacing": "Small"
                        },
                        {
                            "type": "TextBlock",
                            "text": "**Incident Details**",
                            "weight": "Bolder",
                            "spacing": "Medium"
                        }
                    ] + table_rows + ([
                        {
                            "type": "TextBlock",
                            "text": f"[https://bitzer-sandbox.atlassian.net/browse/{jira_key}](https://bitzer-sandbox.atlassian.net/browse/{jira_key})",
                            "wrap": True,
                            "spacing": "Small",
                            "color": "Accent"
                        }
                    ] if jira_key else []) + [
                        {
                            "type": "TextBlock",
                            "text": "_Reported via: AI Monitoring Tool_",
                            "isSubtle": True,
                            "wrap": True,
                            "spacing": "Small"
                        }
                    ] + ([
                        {
                            "type": "ActionSet",
                            "actions": [
                                {
                                    "type": "Action.OpenUrl",
                                    "title": "View JIRA Ticket",
                                    "url": f"https://bitzer-sandbox.atlassian.net/browse/{jira_key}"
                                }
                            ]
                        }
                    ] if jira_key else []),
                    "msteams": {
                        "width": "Full"
                    }
                }
            }]
        }
    
    @log_function_call(notification_logger)
    async def send_notification(self, email_data: EmailData, jira_key: str = None,
                                 machine_name: str = None, category: str = None,
                                 assignee: str = None) -> Dict[str, Any]:
        """
        Send Teams notification to appropriate channel based on TRIGGER NAME matching.
        
        This method now uses trigger-based routing instead of machine-based routing.
        The trigger name from the email is fuzzy-matched against the trigger_mappings
        table to determine the destination Teams channel.
        """
        if not self.enabled:
            return {"success": False, "reason": "Teams disabled"}
        
        # Extract machine name if not provided (still needed for display)
        if not machine_name:
            machine_name = self._extract_machine_name(email_data)
        
        # ==================================================================
        # NEW: Trigger-based routing (replaces machine-based lookup)
        # ==================================================================
        trigger_name = email_data.trigger_name or ""
        team, confidence, matched_trigger,assignee_name = self.trigger_matcher.find_best_match(trigger_name)
        
        
        if confidence >= TriggerMatcher.MATCH_THRESHOLD:
            logger.info(f"   ✅ Trigger match ({confidence*100:.0f}%): '{trigger_name[:50]}' → {team}")
        else:
            logger.info(f"   ⚠️ Low match ({confidence*100:.0f}%): '{trigger_name[:50]}' → {team}")
        
        # If routed to General (no good match found), log to text file
        if team == "General":
            formatted_timestamp = self._format_timestamp(email_data.timestamp)
            self._log_unmatched_trigger(trigger_name, formatted_timestamp)
        
        # Use team as the infrastructure/channel destination
        infrastructure = team
        
        # Get webhook URL for the team/channel
        webhook_url = settings.get_webhook_for_team(team)
        
        if not webhook_url:
            return {"success": False, "reason": f"No webhook for {team}",
                    "infrastructure": infrastructure, "machine": machine_name,
                    "team": team, "confidence": confidence}
        
        # Build and send notification
        payload = self._build_adaptive_card(email_data, jira_key, machine_name, infrastructure, assignee)
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(webhook_url, json=payload)
                
                if response.status_code in (200, 202):
                    return {"success": True, "infrastructure": infrastructure, 
                            "machine": machine_name, "channel": team,
                            "team": team, "confidence": confidence,
                            "status_code": response.status_code}
                else:
                    return {"success": False, "reason": f"HTTP {response.status_code}",
                            "infrastructure": infrastructure, "machine": machine_name,
                            "team": team, "confidence": confidence}
        except Exception as e:
            return {"success": False, "reason": str(e), 
                    "infrastructure": infrastructure, "machine": machine_name,
                    "team": team, "confidence": confidence}


# ==============================================================================
# Commented out legacy code preserved for reference
# ==============================================================================

# class EmailProcessor:
#     """Main email processor with Teams channel routing"""
    
#     def __init__(self):
#         self.teams = TeamsIntegration() if settincgs.MS_TEAMS_ENABLED else None
#         self.db_lookup = DatabaseLookup()
#         logger.info("✅ Teams integration initialized")
    
#     async def close(self):
#         """Cleanup resources"""
#         logger.info("✅ Processor closed")
    
#     def _extract_resource_name(self, email_data: dict) -> Optional[str]:
#         """Extract resource/machine name from email"""
#         if email_data.get('resource_name'):
#             name = email_data['resource_name']
#             if '@' in name:
#                 name = name.split('@')[0]
#             elif '.bitzer' in name.lower():
#                 name = name.split('.')[0]
#             return name.upper()
        
#         subject = email_data.get('subject', '')
#         body = email_data.get('body', '')
        
#         # Improved patterns for machine name extraction
#         patterns = [
#             r'Machine\s+([A-Za-z0-9]+)\.bitzer',
#             r'Computer\s+([A-Za-z0-9]+)\.bitzer',
#             r'on\s+([A-Za-z0-9]+)\s+\(',
#             r'([A-Z]{2}[A-Z0-9]{3,}[0-9]+)',
#         ]
        
#         for pattern in patterns:
#             match = re.search(pattern, subject, re.IGNORECASE)
#             if match:
#                 return match.group(1).upper()
        
#         for pattern in patterns:
#             match = re.search(pattern, body, re.IGNORECASE)
#             if match:
#                 return match.group(1).upper()
        
#         return None
    
#     def _get_short_subject(self, subject: str, max_len: int = 60) -> str:
#         """Truncate subject for display"""
#         if len(subject) <= max_len:
#             return subject
#         return subject[:max_len] + "..."
    
#     async def process_email(self, email_data: dict) -> dict:
#         """Process a single email with channel routing"""
#         subject = email_data.get('subject', 'Unknown')
#         priority = email_data.get('priority', 'Unknown')
#         short_subject = self._get_short_subject(subject)
        
#         result = {
#             "success": False,
#             "subject": subject,
#             "priority": priority,
#             "trigger_name": email_data.get('trigger_name', ''),
#             "jira_ticket": None,
#             "teams_notification_sent": False,
#             "teams_channel": None,
#             "infrastructure": None,
#             "machine_name": None
#         }
        
#         # Print email header
#         logger.info("")
#         logger.info("=" * 70)
#         logger.info(f"📧 Processing: {short_subject}")
#         logger.info(f"   Priority: {priority} (from Kortex)")
        
#         # Only process P1/P2 for Teams notifications
#         if priority not in ('P1', 'P2'):
#             logger.info(f"   Create Jira: No (Priority: {priority})")
#             result["success"] = True
#             result["reason"] = f"Priority {priority} - no notification needed"
#             return result
        
#         logger.info(f"   Create Jira: Yes")
        
#         # Extract machine name
#         machine_name = self._extract_resource_name(email_data)
#         result["machine_name"] = machine_name
        
#         # Get infrastructure from database
#         infrastructure = "General"
#         if machine_name:
#             infrastructures = self.db_lookup.get_infrastructure_for_machine(machine_name)
#             if infrastructures:
#                 infrastructure = InfrastructureRouter.resolve_infrastructure(infrastructures)
#                 logger.info(f"   Machine: {machine_name} → {infrastructure}")
#             else:
#                 logger.info(f"   Machine: {machine_name} → General (not found in DB)")
#         else:
#             logger.info(f"   Machine: Could not extract → General")
        
#         result["infrastructure"] = infrastructure
        
#         # Send Teams notification
#         if self.teams:
#             email_obj = EmailData.from_dict(email_data)
#             teams_result = await self.teams.send_notification(email_obj, machine_name=machine_name)
#             result["teams_notification_sent"] = teams_result.get("success", False)
#             result["teams_channel"] = teams_result.get("infrastructure", "Unknown")
            
#             if teams_result.get("success"):
#                 logger.info(f"   ✅ Teams notification sent to {result['teams_channel']} (status: {teams_result.get('status_code', 'OK')})")
#             else:
#                 logger.info(f"   ❌ Teams notification failed: {teams_result.get('reason')}")
        
#         result["success"] = True
#         return result
    
#     async def process_batch(self, emails: List[dict]) -> List[dict]:
#         """Process a batch of emails"""
#         logger.info(f"\n🚀 Processing batch of {len(emails)} emails")
        
#         results = []
#         for i, email in enumerate(emails, 1):
#             try:
#                 result = await self.process_email(email)
#                 results.append(result)
#             except Exception as e:
#                 logger.error(f"❌ Error processing email {i}: {e}")
#                 results.append({"success": False, "error": str(e), 
#                                "subject": email.get('subject', 'Unknown')})
        
#         return results


# def print_summary(results: List[dict]):
#     """Print processing summary"""
#     print("\n" + "="*70)
#     print("📊 PROCESSING SUMMARY")
#     print("="*70)
    
#     total = len(results)
#     success = sum(1 for r in results if r.get('success'))
#     teams_sent = sum(1 for r in results if r.get('teams_notification_sent'))
    
#     # Count by priority
#     p1_count = sum(1 for r in results if r.get('priority') == 'P1')
#     p2_count = sum(1 for r in results if r.get('priority') == 'P2')
#     info_count = sum(1 for r in results if r.get('priority') == 'Informational')
    
#     print(f"Total processed:     {total}")
#     print(f"Successful:          {success}")
#     print(f"Teams notifications: {teams_sent}")
#     print(f"\nBy Priority:")
#     print(f"   P1:              {p1_count}")
#     print(f"   P2:              {p2_count}")
#     print(f"   Informational:   {info_count}")
    
#     # Group by infrastructure
#     infra_counts = {}
#     for r in results:
#         if r.get('teams_notification_sent'):
#             infra = r.get('teams_channel') or r.get('infrastructure') or 'Unknown'
#             infra_counts[infra] = infra_counts.get(infra, 0) + 1
    
#     if infra_counts:
#         print("\nTeams by Channel:")
#         for infra, count in sorted(infra_counts.items()):
#             print(f"   {infra}: {count}")
    
#     # List machines not found
#     not_found = [r for r in results if r.get('teams_notification_sent') and r.get('teams_channel') == 'General' and r.get('machine_name')]
#     if not_found:
#         print(f"\n⚠️  Machines routed to General (not in DB):")
#         for r in not_found:
#             print(f"   - {r.get('machine_name')}")
    
#     print("="*70)