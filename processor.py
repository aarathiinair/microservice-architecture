#!/usr/bin/env python3
"""
KPMG Email Processing System - Integrated Version
Combines: Channel Routing + Jira Ticket Creation + Teams Notifications
"""

import asyncio
import logging
import json
import re
import httpx
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from difflib import SequenceMatcher

from jira import JIRA
from config import settings
from db_schema2 import get_db, Server, TriggerMapping
from sqlalchemy.orm import Session

# Setup logging
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


# =============================================================================
# NEW: TriggerMatcher for fuzzy trigger_name → Team channel matching
# =============================================================================
class TriggerMatcher:
    """Fuzzy matching for trigger names to Teams channels"""
    
    MATCH_THRESHOLD = 0.75  # 75% similarity threshold
    
    def __init__(self):
        self._cache: Dict[str, tuple] = {}
        self._db_triggers: List[tuple] = []
        self._load_mappings()
    
    def _load_mappings(self):
        """Load all trigger mappings from DB"""
        try:
            db: Session = next(get_db())
            try:
                mappings = db.query(TriggerMapping).all()
                self._db_triggers = [(m.trigger_name, m.team) for m in mappings]
                logger.info(f"✅ Loaded {len(self._db_triggers)} trigger mappings")
            finally:
                db.close()
        except Exception as e:
            logger.warning(f"⚠️ Could not load trigger mappings: {e}")
    
    def _normalize(self, text: str) -> str:
        """Normalize trigger name for comparison"""
        text = text.lower()
        text = re.sub(r'[:\-_<>]', ' ', text)
        text = re.sub(r'controlup://\S+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    def _similarity(self, email_trigger: str, db_trigger: str) -> float:
        """Calculate similarity between email trigger and DB trigger"""
        norm_email = self._normalize(email_trigger)
        norm_db = self._normalize(db_trigger)
        
        # Sequence matching
        seq_ratio = SequenceMatcher(None, norm_email, norm_db).ratio()
        
        # Token overlap
        skip = {'the', 'a', 'an', 'on', 'in', 'is', 'and', 'or', 'to', 'for', 'of'}
        tokens_email = {w for w in norm_email.split() if len(w) > 2 and w not in skip}
        tokens_db = {w for w in norm_db.split() if len(w) > 2 and w not in skip}
        
        if tokens_email and tokens_db:
            intersection = tokens_email & tokens_db
            union = tokens_email | tokens_db
            token_ratio = len(intersection) / len(union) if union else 0
        else:
            token_ratio = 0
        
        return max(seq_ratio, token_ratio * 1.1)
    
    def get_team_for_trigger(self, trigger_name: str) -> tuple:
        """Returns: (team, confidence, matched_db_trigger)"""
        if not trigger_name or not self._db_triggers:
            return ("General", 0.0, "")
        
        if trigger_name in self._cache:
            team, db_trigger = self._cache[trigger_name]
            return (team, 1.0, db_trigger)
        
        best_match = ("General", 0.0, "")
        
        for db_trigger, team in self._db_triggers:
            score = self._similarity(trigger_name, db_trigger)
            if score > best_match[1]:
                best_match = (team, score, db_trigger)
            if score >= 0.95:
                break
        
        if best_match[1] >= self.MATCH_THRESHOLD:
            self._cache[trigger_name] = (best_match[0], best_match[2])
        
        return best_match


class InfrastructureRouter:
    """Handles routing logic for infrastructure selection"""
    
    @staticmethod
    def resolve_infrastructure(groups: List[str]) -> str:
        if not groups:
            return "General"
        if len(groups) == 1:
            return groups[0]
        groups_lower = [g.lower() for g in groups]
        for i, g in enumerate(groups_lower):
            if "oi-rda" in g:
                return groups[i]
        oi_ibs_idx = None
        for i, g in enumerate(groups_lower):
            if "oi-ibs" in g:
                oi_ibs_idx = i
                break
        if oi_ibs_idx is not None:
            for i, g in enumerate(groups):
                if i != oi_ibs_idx:
                    return g
        return groups[0]


class DatabaseLookup:
    """Handles database lookups for machine-to-infrastructure mapping"""
    
    @staticmethod
    def get_infrastructure_for_machine(machine_name: str) -> List[str]:
        if not machine_name:
            return []
        try:
            db: Session = next(get_db())
            try:
                results = db.query(Server).filter(
                    Server.computername.ilike(machine_name)
                ).all()
                if results:
                    return list(set(r.group for r in results if r.group))
                return []
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Database lookup error for {machine_name}: {e}")
            return []


class JiraIntegration:
    """Handles Jira ticket creation"""
    
    def __init__(self):
        self.jira_client = None
        self.executor = ThreadPoolExecutor(max_workers=5)
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize JIRA client"""
        if not settings.JIRA_BASE_URL or not settings.JIRA_EMAIL or not settings.JIRA_API_TOKEN:
            logger.warning("⚠️  Jira not configured - tickets will not be created")
            return
            
        try:
            self.jira_client = JIRA(
                server=settings.JIRA_BASE_URL,
                basic_auth=(settings.JIRA_EMAIL, settings.JIRA_API_TOKEN)
            )
            logger.info("✅ Jira client initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Jira: {e}")
    
    def _extract_machine_name(self, text: str) -> str:
        """Extract machine name from text"""
        machine_pattern = r'(DE[A-Z]{2,4}\d{5,6})'
        match = re.search(machine_pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
        
        bitzer_pattern = r'(\w+)\.bitzer\.biz'
        match = re.search(bitzer_pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
        
        return "Unknown"
    
    def _convert_priority_to_jira(self, priority: str) -> str:
        """Convert priority to Jira priority names"""
        mapping = {
            "P1": "Highest",
            "P2": "High",
            "Informational": "Low",
            "NA": "Lowest"
        }
        return mapping.get(priority, "Medium")
    
    def _build_description(self, email_data: EmailData, machine_name: str) -> str:
        """Build Jira ticket description"""
        body = email_data.body
        description = f"*Alert Details*\n\n"
        description += f"*Machine:* {machine_name}\n"
        description += f"*Trigger:* {email_data.trigger_name}\n"
        description += f"*Timestamp:* {email_data.timestamp}\n"
        description += f"*Priority:* {email_data.priority}\n\n"
        description += f"*Email Body:*\n{body[:1000]}"
        return description
    
    async def create_ticket(self, email_data: EmailData, machine_name: str) -> Optional[tuple]:
        """Create Jira ticket and return (ticket_key, assignee_name)"""
        if not self.jira_client:
            logger.warning("⚠️  Jira not configured - skipping ticket creation")
            return None
        
        try:
            loop = asyncio.get_event_loop()
            jira_priority = self._convert_priority_to_jira(email_data.priority)
            description = self._build_description(email_data, machine_name)
            
            issue_type_name = settings.JIRA_ISSUE_TYPE.replace('[', '').replace(']', '')
            
            ticket = await loop.run_in_executor(
                self.executor,
                lambda: self.jira_client.create_issue(
                    project=settings.JIRA_PROJECT_KEY,
                    summary=f"[{email_data.priority}] {email_data.trigger_name} - {machine_name}",
                    description=description,
                    issuetype={'name': issue_type_name},
                    priority={'name': jira_priority}
                )
            )
            
            assignee_name = "Team"
            if hasattr(ticket.fields, 'assignee') and ticket.fields.assignee:
                assignee_name = ticket.fields.assignee.displayName
            
            logger.info(f"   ✅ Jira ticket created: {ticket.key} (Assignee: {assignee_name})")
            return (ticket.key, assignee_name)
            
        except Exception as e:
            logger.error(f"   ❌ Jira ticket creation failed: {e}")
            return None
    
    async def close(self):
        """Clean up resources"""
        self.executor.shutdown(wait=False)


class TeamsIntegration:
    """Handles Teams notifications with trigger-based channel routing"""
    
    def __init__(self):
        self.trigger_matcher = TriggerMatcher()  # NEW: Use trigger matching
        self.enabled = settings.MS_TEAMS_ENABLED
    
    def _extract_machine_name(self, email_data: EmailData) -> Optional[str]:
        """Extract machine/resource name from email data"""
        if email_data.resource_name:
            name = email_data.resource_name
            if '@' in name:
                name = name.split('@')[0]
            elif '.' in name and '.bitzer' in name.lower():
                name = name.split('.')[0]
            return name.upper()
        
        subject = email_data.subject
        patterns = [
            r'Machine\s+([A-Za-z0-9]+)\.bitzer',
            r'Computer\s+([A-Za-z0-9]+)\.bitzer',
            r'on\s+([A-Za-z0-9]+)\s+\(',
            r'([A-Z]{2}[A-Z0-9]{3,}[0-9]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, subject, re.IGNORECASE)
            if match:
                return match.group(1).upper()
        
        for pattern in patterns:
            match = re.search(pattern, email_data.body, re.IGNORECASE)
            if match:
                return match.group(1).upper()
        
        return None
    
    def _format_timestamp(self, timestamp: Optional[str]) -> str:
        """Format timestamp for Teams notification"""
        if not timestamp:
            return datetime.now().strftime("%Y-%m-%d %H:%M CST")
        
        try:
            dt = datetime.fromisoformat(timestamp.replace('+00:00', '+0000'))
            return dt.strftime("%Y-%m-%d %H:%M CST")
        except:
            return timestamp
    
    def _extract_clean_sender(self, sender: str) -> str:
        """Extract clean email address from sender string"""
        email_pattern = r'<?([\w\.-]+@[\w\.-]+)>?'
        match = re.search(email_pattern, sender)
        if match:
            return match.group(1)
        return sender.replace('"', '').strip()
    
    async def send_notification(self, email_data: EmailData, jira_key: str = None,
                                machine_name: str = None, assignee_name: str = "Team") -> Dict:
        """Send Teams notification to appropriate channel based on trigger name"""
        
        if not self.enabled:
            return {"success": False, "reason": "Teams disabled"}
        
        if not machine_name:
            machine_name = self._extract_machine_name(email_data)
        
        # NEW: Get team/channel from trigger matching
        team, confidence, matched_trigger = self.trigger_matcher.get_team_for_trigger(email_data.trigger_name)
        
        if confidence < TriggerMatcher.MATCH_THRESHOLD:
            logger.info(f"   ⚠️ Low match ({confidence:.0%}): '{email_data.trigger_name}' → General")
            team = "General"
        else:
            logger.info(f"   🎯 Trigger match ({confidence:.0%}): '{matched_trigger}' → {team}")
        
        webhook_url = settings.get_webhook_for_team(team)
        
        if not webhook_url:
            return {"success": False, "reason": f"No webhook for {team}",
                    "team": team, "machine": machine_name}
        
        # Build and send notification - ORIGINAL ADAPTIVE CARD FORMAT
        try:
            clean_sender = self._extract_clean_sender(email_data.sender)
            formatted_timestamp = self._format_timestamp(email_data.timestamp)
            jira_url = f"{settings.JIRA_BASE_URL}/browse/{jira_key}" if jira_key else "N/A"
            
            table_data = [
                {"col1": "Source", "col2": clean_sender},
                {"col1": "Resource Name", "col2": machine_name or "Unknown"},
                {"col1": "Trigger Name", "col2": email_data.trigger_name},
                {"col1": "Priority", "col2": email_data.priority},
                {"col1": "Incident Timestamp", "col2": formatted_timestamp},
                {"col1": "JIRA Ticket", "col2": jira_key or "N/A"}
            ]
            
            table_rows = []
            table_rows.append({
                "type": "ColumnSet",
                "columns": [
                    {"type": "Column", "width": "150px", "items": [{"type": "TextBlock", "text": "**Details**", "weight": "Bolder"}]},
                    {"type": "Column", "width": "350px", "items": [{"type": "TextBlock", "text": "**Value**", "weight": "Bolder"}]}
                ],
                "separator": True
            })
            
            for row in table_data:
                table_rows.append({
                    "type": "ColumnSet",
                    "columns": [
                        {"type": "Column", "width": "150px", "items": [{"type": "TextBlock", "text": row["col1"], "wrap": True}]},
                        {"type": "Column", "width": "350px", "items": [{"type": "TextBlock", "text": row["col2"], "wrap": True}]}
                    ],
                    "separator": True
                })
            
            message = {
                "type": "message",
                "attachments": [{
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {"type": "Container", "style": "emphasis", "items": [{"type": "TextBlock", "text": "MS Teams Incident Notification", "weight": "Bolder", "size": "Large"}], "bleed": True},
                            {"type": "TextBlock", "text": f"**Incident Notification: {email_data.subject}**", "wrap": True, "spacing": "Medium", "size": "Medium", "weight": "Bolder"},
                            {"type": "TextBlock", "text": f"Hi {assignee_name},", "wrap": True, "spacing": "Medium"},
                            {"type": "TextBlock", "text": "The ControlUp monitoring system has reported an incident. Please review the details below and take appropriate action:", "wrap": True, "spacing": "Small"},
                            {"type": "TextBlock", "text": "**Incident Details**", "weight": "Bolder", "spacing": "Medium"}
                        ] + table_rows + [
                            {"type": "TextBlock", "text": f"[{jira_url}]({jira_url})", "wrap": True, "spacing": "Medium"},
                            {"type": "TextBlock", "text": "_Reported via: AI Monitoring Tool_", "isSubtle": True, "wrap": True, "spacing": "Small"}
                        ],
                        "actions": [{"type": "Action.OpenUrl", "title": "View JIRA Ticket", "url": jira_url}],
                        "msteams": {"width": "Full"}
                    }
                }]
            }
            
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                response = await client.post(webhook_url, json=message)
                
                if response.status_code in (200, 202):
                    return {"success": True, "team": team, "machine": machine_name, 
                            "channel": team, "status_code": response.status_code}
                else:
                    return {"success": False, "reason": f"HTTP {response.status_code}",
                            "team": team, "machine": machine_name}
        except Exception as e:
            return {"success": False, "reason": str(e), "team": team, "machine": machine_name}


class EmailProcessor:
    """Main email processor with channel routing and Jira integration"""
    
    def __init__(self):
        self.teams = TeamsIntegration() if settings.MS_TEAMS_ENABLED else None
        self.jira = JiraIntegration()
        self.db_lookup = DatabaseLookup()
        logger.info("✅ Email Processor initialized (Teams + Jira + Trigger-based Routing)")
    
    async def close(self):
        if self.jira:
            await self.jira.close()
        logger.info("✅ Processor closed")
    
    def _extract_resource_name(self, email_data: dict) -> Optional[str]:
        if email_data.get('resource_name'):
            name = email_data['resource_name']
            if '@' in name:
                name = name.split('@')[0]
            elif '.bitzer' in name.lower():
                name = name.split('.')[0]
            return name.upper()
        
        subject = email_data.get('subject', '')
        body = email_data.get('body', '')
        patterns = [
            r'Machine\s+([A-Za-z0-9]+)\.bitzer',
            r'Computer\s+([A-Za-z0-9]+)\.bitzer',
            r'on\s+([A-Za-z0-9]+)\s+\(',
            r'([A-Z]{2}[A-Z0-9]{3,}[0-9]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, subject, re.IGNORECASE)
            if match:
                return match.group(1).upper()
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                return match.group(1).upper()
        
        return None
    
    def _get_short_subject(self, subject: str, max_len: int = 60) -> str:
        if len(subject) <= max_len:
            return subject
        return subject[:max_len] + "..."
    
    async def process_email(self, email_data: dict) -> dict:
        subject = email_data.get('subject', 'Unknown')
        priority = email_data.get('priority', 'Unknown')
        short_subject = self._get_short_subject(subject)
        
        result = {
            "success": False, "subject": subject, "priority": priority,
            "trigger_name": email_data.get('trigger_name', ''),
            "jira_ticket": None, "teams_notification_sent": False,
            "teams_channel": None, "infrastructure": None, "machine_name": None
        }
        
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"📧 Processing: {short_subject}")
        logger.info(f"   Priority: {priority} (from Kortex)")
        
        if priority not in ('P1', 'P2'):
            logger.info(f"   Create Jira: No (Priority: {priority})")
            result["success"] = True
            result["reason"] = f"Priority {priority} - no notification needed"
            return result
        
        logger.info(f"   Create Jira: Yes")
        
        machine_name = self._extract_resource_name(email_data)
        result["machine_name"] = machine_name
        
        jira_ticket_key = None
        assignee_name = "Team"
        if self.jira and self.jira.jira_client:
            email_obj = EmailData.from_dict(email_data)
            jira_result = await self.jira.create_ticket(email_obj, machine_name or "Unknown")
            if jira_result:
                jira_ticket_key, assignee_name = jira_result
                result["jira_ticket"] = jira_ticket_key
        
        if self.teams:
            email_obj = EmailData.from_dict(email_data)
            teams_result = await self.teams.send_notification(
                email_obj, jira_key=jira_ticket_key,
                machine_name=machine_name, assignee_name=assignee_name
            )
            result["teams_notification_sent"] = teams_result.get("success", False)
            result["teams_channel"] = teams_result.get("team", "Unknown")
            result["infrastructure"] = teams_result.get("team", "Unknown")
            
            if teams_result.get("success"):
                logger.info(f"   ✅ Teams notification sent to {result['teams_channel']} (status: {teams_result.get('status_code', 'OK')})")
            else:
                logger.info(f"   ❌ Teams notification failed: {teams_result.get('reason')}")
        
        result["success"] = True
        return result
    
    async def process_batch(self, emails: List[dict]) -> List[dict]:
        logger.info(f"\n🚀 Processing batch of {len(emails)} emails")
        results = []
        for i, email in enumerate(emails, 1):
            try:
                result = await self.process_email(email)
                results.append(result)
            except Exception as e:
                logger.error(f"❌ Error processing email {i}: {e}")
                results.append({"success": False, "error": str(e), 
                               "subject": email.get('subject', 'Unknown')})
        return results


def print_summary(results: List[dict]):
    print("\n" + "="*70)
    print("📊 PROCESSING SUMMARY")
    print("="*70)
    
    total = len(results)
    success = sum(1 for r in results if r.get('success'))
    teams_sent = sum(1 for r in results if r.get('teams_notification_sent'))
    jira_created = sum(1 for r in results if r.get('jira_ticket'))
    
    p1_count = sum(1 for r in results if r.get('priority') == 'P1')
    p2_count = sum(1 for r in results if r.get('priority') == 'P2')
    info_count = sum(1 for r in results if r.get('priority') == 'Informational')
    
    print(f"Total processed:     {total}")
    print(f"Successful:          {success}")
    print(f"Jira tickets:        {jira_created}")
    print(f"Teams notifications: {teams_sent}")
    print(f"\nBy Priority:")
    print(f"   P1:              {p1_count}")
    print(f"   P2:              {p2_count}")
    print(f"   Informational:   {info_count}")
    
    channel_counts = {}
    for r in results:
        if r.get('teams_notification_sent'):
            ch = r.get('teams_channel') or r.get('infrastructure') or 'Unknown'
            channel_counts[ch] = channel_counts.get(ch, 0) + 1
    
    if channel_counts:
        print("\nTeams by Channel:")
        for ch, count in sorted(channel_counts.items()):
            print(f"   {ch}: {count}")
    
    print("="*70)