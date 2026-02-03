import re
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum
import pandas as pd

class Priority(Enum):
    P1 = "P1"  # Critical
    P2 = "P2"  # High
    INFORMATIONAL = "Informational"  # Informational only
    NA = "NA"  # Not Applicable

class ActionType(Enum):
    IMMEDIATE = "Immediate Action Required"
    SCHEDULED = "Scheduled Action Required"
    INFORMATIONAL = "Informational"
    MONITOR = "Monitor Only"

class Category(Enum):
    CITRIX = "CITRIX"
    HYPERVISOR_VMWARE = "Hypervisor/VMware"
    SAP = "SAP"
    OI_IBS = "OI-IBS"
    OI_RDA = "OI-RDA" 
    OI_SQL = "OI-SQL"
    OI_ROT = "OI-ROT"
    APPLICATION_SERVERS = "Application Servers"
    ADC = "ADC"
    BITZER = "BITZER"
    CONTROLUP = "ControlUp"
    LINUX = "Linux"
    SERVICE_TRIGGER = "Service Trigger"
    SONSTIGE = "Sonstige"

@dataclass
class Rule:
    name: str
    patterns: List[str]  # Regex patterns to match
    priority: Priority
    action_type: ActionType
    create_jira: bool
    conditions: Optional[Dict[str, Any]] = None

class EmailRulesEngine:
    def __init__(self):
        self.rules = self._initialize_rules()
    
    def _initialize_rules(self) -> List[Rule]:
        """Initialize predefined rules based on the email patterns seen in the data"""
        return [
            # Machine Shutdown Gracefully - Informational Priority
            Rule(
                name="Machine Shutdown Gracefully",
                patterns=[
                    r"machine.*shut.*down.*gracefully",
                    r"shut.*down.*gracefully"
                ],
                priority=Priority.INFORMATIONAL,
                action_type=ActionType.INFORMATIONAL,
                create_jira=False
            ),
            
            # Computer Down Rules
            Rule(
                name="Computer Down",
                patterns=[
                    r"machine.*down(?!.*gracefully)",
                    r"computer.*down(?!.*gracefully)",
                    r"server.*down(?!.*gracefully)",
                    r"machine.*unreachable"
                ],
                priority=Priority.P1,
                action_type=ActionType.IMMEDIATE,
                create_jira=True
            ),

            # Service Down Rules
            Rule(
                name="Service Down/Stopped",
                patterns=[
                    r"\\bservice.*(down|stopped)\\b",
                    r"\\bpvs.*service.*(down|stopped)\\b",
                    r"\\bjira.*tomcat.*(down|stopped)\\b",
                    r"\\bxframe.*service.*down\\b",
                    r"\\bwis.*service.*stopped\\b",
                    r"\\bshopfloorengine.*service.*stopped\\b"
                ],
                priority=Priority.P1,
                action_type=ActionType.IMMEDIATE,
                create_jira=True
            ),
            
            # Critical Resource Exhaustion Rules
            Rule(
                name="Critical Resource Exhaustion",
                patterns=[
                    r"cpu.*greater.*than.*equal.*95",
                    r"memory.*utilization.*greater.*than.*equal.*95",
                    r"disk.*queue.*greater.*than.*equal.*(1|5|21)",
                    r"vcpu.*pcpu.*ratio.*greater.*than.*equal.*3",
                    r"storage.*latency.*greater.*than.*equal.*100.*ms"
                ],
                priority=Priority.P1,
                action_type=ActionType.IMMEDIATE,
                create_jira=True
            ),

            # Network / Load Balancer Degraded Rules
            Rule(
                name="Network / Load Balancer Degraded",
                patterns=[
                    r"adc.*storefront.*lb.*degraded",
                    r"adc.*exchange.*lb.*(rpc|active\\s*sync|owa).*degraded",
                    r"adc.*wem.*services.*lb.*degraded"
                ],
                priority=Priority.P1,
                action_type=ActionType.IMMEDIATE,
                create_jira=True
            ),
            
            # CIRTIX Critical Errors Rules
            Rule(
                name="CITRIX Critical Error",
                patterns=[
                    r"fslogix.*profile.*corrupted",
                    r"fslogix.*error",
                    r"broker.*cannot.*find.*(any|available).*vm",
                    r"winlogon.*error",
                    r"xendesktop.*error.*event"
                ],
                priority=Priority.P1,
                action_type=ActionType.IMMEDIATE,
                create_jira=True
            ),
            
            # Low Disk Space Warnings Rules
            Rule(
                name="Low Disk Space Warning",
                patterns=[
                    r"(less.*(5|15).*gb)",
                    r"(free.*space.*(<=|less.*than).*(5|15).*gb)",
                    r"(less.*10.*percent)",
                    r"(free.*capacity.*(<=|less.*than).*10%)",
                    r"linux.*disk.*less.*20.*percent",
                    r"linux.*free.*space.*(<=|less.*than).*20.*percent",
                    r"vda.*d:\\\\.*free.*space.*(<=|less.*than).*2.*gb"
                ],
                priority=Priority.P2,
                action_type=ActionType.SCHEDULED,
                create_jira=True
            ),
            
            # Warnings on System Health Rules
            Rule(
                name="Warnings on System Health",
                patterns=[
                    r"memory.*ballooning.*(greater.*than.*equal|>=).*10.*mb",
                    r"exchange.*(memory|cpu).*monitor",
                    r"application.*servers.*less.*(5|10).*gb.*and.*less.*10.*percent"
                ],
                priority=Priority.P2,
                action_type=ActionType.SCHEDULED,
                create_jira=True
            ),
            
            # Services Restored Rules
            Rule(
                name="Services Up / Restored",
                patterns=[
                    r"adc.*exchange.*lb.*rpc.*restored",
                    r"adc.*exchange.*lb.*active\\s*sync.*restored",
                    r"adc.*storefront.*lb.*restored",
                    r"adc.*exchange.*lb.*owa.*restored",
                    r"adc.*wem.*services.*lb.*restored",
                    r"xframe.*service.*up",
                    r"wis.*service.*started",
                    r"shopfloorengine.*pr3.*service.*started",
                    r"citrix.*license.*service.*up",
                    r"citrix.*wem.*service.*up",
                    r"monitor.*delivery.*controller.*service.*up"
                ],
                priority=Priority.P2,
                action_type=ActionType.SCHEDULED,
                create_jira=True
            ),
            
            # Informational Rules
            Rule(
                name="Informational",
                patterns=[
                    r"windows.*event.*custom.*filter",
                    r"scoutbees.*services",
                    r"glt.*services",
                    r"adc.*certificate.*expiration",
                    r"citrix.*storefront.*test",
                    r"citrix.*xenapp.*restore",
                    r"vmware.*horizon.*connection.*server.*health",
                    r"vmware.*horizon.*connection.*server.*events",
                    r"vmware.*horizon.*connection.*server.*process",
                    r"linux.*xterm.*process.*ended",
                    r"sap.*basis.*proc.*ended.*bellin.*client.*transport\\.exe",
                    r"bitzer.*machine.*down.*custom.*filter",
                    r"adc.*netscaler.*schwenk",
                    r"exchange.*db.*schwenk",
                    r"exchange.*db.*schwenk.*v02",
                    r"printserver.*event.*id.*4009",
                    r"exchange.*fehler.*zustellung.*event.*id.*(15004|15006)",
                    r"exchange.*fehler.*zustellung.*event.*id.*(15004|15006).*v03"
                ],
                priority=Priority.INFORMATIONAL,
                action_type=ActionType.INFORMATIONAL,
                create_jira=False
            )
        ]
    
    def _extract_machine_name(self, email_subject: str) -> str:
        """Extract machine name from email subject"""
        # Look for patterns like DESDN01057, DEROT04428, etc.
        machine_pattern = r'(DE[A-Z]{2,4}\d{5,6})'
        match = re.search(machine_pattern, email_subject, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Look for .bitzer.biz patterns
        bitzer_pattern = r'(\w+\.bitzer\.biz)'
        match = re.search(bitzer_pattern, email_subject, re.IGNORECASE)
        if match:
            return match.group(1)
        
        return "Unknown"
    
    def _extract_category_from_body(self, email_body: str) -> Optional[Category]:
        """
        Extract category from the email body based on the trigger name format.
        Assumes category is the first word after 'Trigger name:'.
        """
        pattern = r"trigger name:\s*([A-Z0-9_\-]+)"
        match = re.search(pattern, email_body, re.IGNORECASE)
        if match:
            first_word = match.group(1).upper()
            for cat in Category:
                if first_word == cat.name:
                    return cat
        return None

    def _generate_jira_summary(self, email_subject: str, category: Category) -> str:
        """Generate JIRA ticket summary"""
        # Truncate subject if too long
        max_length = 80
        if len(email_subject) > max_length:
            email_subject = email_subject[:max_length] + "..."
        
        return f"[{category.value}] {email_subject}"
    
    def _match_rule(self, email_data: Dict[str, Any], rule: Rule) -> bool:
        """Check if email data matches a specific rule"""
        email_subject = email_data.get('subject', '').lower()
        email_body = email_data.get('body', '').lower()
        
        # Combine subject and body for pattern matching
        combined_text = f"{email_subject} {email_body}"
        
        # Check if any pattern matches
        for pattern in rule.patterns:
            if re.search(pattern, combined_text, re.IGNORECASE):
                return True
        
        return False
    
    def process_email(self, email_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process email data and return action recommendations
        
        Args:
            email_data: Dictionary containing email information
                - subject: Email subject line
                - body: Email body content
                - sender: Email sender
                - timestamp: When email was received
        
        Returns:
            Dictionary with rule matching results and recommended actions
        """
        
        # Find matching rule
        matched_rule = None
        for rule in self.rules:
            if self._match_rule(email_data, rule):
                matched_rule = rule
                break
        
        # If no specific rule matches, use default
        if not matched_rule:
            matched_rule = Rule(
                name="Default Email Processing",
                patterns=[],
                priority=Priority.INFORMATIONAL,
                action_type=ActionType.INFORMATIONAL,
                create_jira=False
            )
        
        # Extract additional information
        machine_name = self._extract_machine_name(email_data.get('subject', ''))
        # trigger_category = self._extract_category_from_body(email_data.get('body', ''))

        # Build response
        response = {
            "rule_matched": matched_rule.name,
            "priority": matched_rule.priority.value,
            "action_type": matched_rule.action_type.value,
            "create_jira": matched_rule.create_jira,
            # "category": trigger_category.name,
            "machine_name": machine_name,
            "email_metadata": {
                "subject": email_data.get('subject', ''),
                "sender": email_data.get('sender', ''),
                "timestamp": email_data.get('timestamp', '')
            }
        }
        
        # Add JIRA ticket details if ticket should be created
        # if matched_rule.create_jira:
        #     response["jira_ticket"] = {
        #         "summary": self._generate_jira_summary(
        #             email_data.get('subject', ''), 
        #             trigger_category
        #         ),
        #         "priority": matched_rule.priority.value,
        #         "issue_type": "Incident" if matched_rule.priority in [Priority.P1, Priority.P2] else "Task",
        #         "description": f"Automated ticket created from email alert.\n\nOriginal Subject: {email_data.get('subject', '')}\nMachine: {machine_name}\nSender: {email_data.get('sender', '')}",
        #         "labels": [trigger_category.value.lower(), "automated", "email_alert"]
        #     }
        
        return response

def process_excel(file_path: str, output_path: str):
    engine = EmailRulesEngine()
    
    # Read the 'Categorized' sheet
    df = pd.read_excel(file_path, sheet_name="Categorized", engine="openpyxl")
    
    # Apply rule classification to the 'Triggers' column
    def classify(trigger_text: str) -> Priority:
        email_data = {
            "subject": trigger_text,
            "body": trigger_text,
            "sender": "",
            "timestamp": ""
        }
        result = engine.process_email(email_data)
        return result["priority"]
    
    df["Priority"] = df["Trigger"].apply(classify)
    df["Informational/Actionable"] = df["Priority"].apply(lambda p: 0 if p == "Informational" else 1)
    
    # Save the modified file
    df.to_excel(output_path, index=False)

def main():
    input_path = r"C:\Users\pulkitmathur\Bitzer\emailProcessingServer\Control_Up_Trigger_List.xlsx"  # Replace with actual path
    output_path = r"C:\Users\pulkitmathur\Bitzer\emailProcessingServer\app\TriggerDB.xlsx"
    
    process_excel(input_path, output_path)
    print(f"Processed file saved to: {output_path}")


if __name__ == "__main__":
    main()
