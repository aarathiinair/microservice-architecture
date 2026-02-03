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





class CertificateTeamsIntegration:
    """Handles Teams notifications with trigger-based channel routing"""
    
    def __init__(self):
        self.trigger_matcher = TriggerMatcher()
        self.enabled = settings.MS_TEAMS_ENABLED
        self.unmatched_triggers_log = "logs/unmatched_triggers.txt"

   
    
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
    def _build_adaptive_card(self, certificate_data, jira_key: str = None,
                             infrastructure: str = None,
                             assignee: str = None) -> Dict:
        """Build Adaptive Card payload for Power Automate webhook"""
        
        # Clean sender email

        
        # Build table rows data
        table_data = [
            {"col1": "Source", "col2": "Certificate Wathcer"},
            {"col1": "Ceritficate Name", "col2": certificate_data.get("certificate_name",None) or "N/A"},
            {"col1": "Description", "col2": certificate_data.get("description",None)},
            {"col1": "Priority", "col2": "P1"},
            {"col1": "Expiration Timestamp", "col2": f"{certificate_data.get("expiration_timestamp",None)}"},
        ]
        
        if infrastructure:
            table_data.append({"col1": "Infrastructure", "col2": certificate_data.get("expiration_timestamp",None)})
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
                            "text": f"**Incident Notification: Certificate Expiry**",
                            "wrap": True,
                            "spacing": "Medium",
                            "size": "Medium",
                            "weight": "Bolder"
                        },
                        {
                            "type": "TextBlock",
                            "text": f"Hi {greeting_name} Team,",
                            "wrap": True,
                            "spacing": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": "The Certificate monitoring system has reported an incident. Please review the details below and take appropriate action:",
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
                    ] if jira_key else []) + ([
                        {
                            "type": "ActionSet",
                            "actions": [
                                {
                                    "type": "Action.OpenUrl",
                                    "title": "Certificate Management URL",
                                    "url": f"https://monitoring-dev.bitzer.biz"
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
    async def send_notification(self, certificate_details, jira_key: str = None) -> Dict[str, Any]:
        """
        Send Teams notification to appropriate channel based on TRIGGER NAME matching.
        
        This method now uses trigger-based routing instead of machine-based routing.
        The trigger name from the email is fuzzy-matched against the trigger_mappings
        table to determine the destination Teams channel.
        """
        if not self.enabled:
            return {"success": False, "reason": "Teams disabled"}
        
        
        # If routed to General (no good match found), log to text file
         
        responsible_group=certificate_details.get("responsible_group",None)
        team_channel=certificate_details.get("team_channel",None)
        

        # Get webhook URL for the team/channel
        webhook_url = settings.get_webhook_for_team(team=None)
        
        if not webhook_url:
            return {"success": False, "reason": f"No webhook for {team_channel}",
                    
                    "team": team_channel,}
        
        # Build and send notification
        payload = self._build_adaptive_card(certificate_details, jira_key,  None, team_channel)
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(webhook_url, json=payload)
                
                if response.status_code in (200, 202):
                    return {"success": True, "infrastructure": responsible_group, "channel": team_channel,
                            "status_code": response.status_code}
                else:
                    return {"success": True, "infrastructure": responsible_group, "channel": team_channel,
                            "status_code": response.status_code}
        except Exception as e:
            return {"success": False, "reason": str(e), 
                     "infrastructure": responsible_group, "channel": team_channel,
                            "status_code": response.status_code}

