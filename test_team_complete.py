#!/usr/bin/env python3
"""
Complete Test Script: Jira Team Field Assignment
Working format: Just UUID string directly

Usage:
    python test_team_complete.py --create --team "BEST Service"
    python test_team_complete.py --create --team "OI - IBS"
    python test_team_complete.py --json email_01_P2_2025-08-27_10-09-29_00-00.json --team "SAP Basis"
    python test_team_complete.py --list-teams
"""

import argparse
import json
import re
import requests
from requests.auth import HTTPBasicAuth
from jira import JIRA

# ============================================================
# CONFIGURATION
# ============================================================
JIRA_BASE_URL = "https://bitzer-sandbox.atlassian.net"
JIRA_EMAIL = "monitoring.ai@bitzer.de"
JIRA_API_TOKEN = "paste_token_here"
JIRA_PROJECT_KEY = "MAI"

# Team Field ID (discovered)
TEAM_FIELD_ID = "customfield_10001"

# ============================================================
# TEAM NAME → UUID MAPPING
# From Jira autocomplete API
# ============================================================
TEAM_ID_MAP = {
    # Trigger mappings teams (from Excel)
    "IBS - CITRIX": None,  # Not found in Jira yet - client needs to create
    "IBS - Virtual Server Infrastructure": "be18814d-a872-432f-9d48-aa8a41b61b80",
    "IBS - Mail Service": "og-82d9c204-17c0-46fb-a396-b412a2eb857e",
    "IBS - Backup": None,  # Not found - client needs to create
    "IBS - ROT": "eda8c020-1ee2-490b-bde6-baa2ef36269d",
    "SAP Basis": "cbc86a6e-8c12-4e3a-8ecd-d4c52b83b17b",
    "SAP Sales": "4c652e69-e207-4e98-b4bf-ca90838de87b",
    "SAP Operations": "c066a998-37cd-4f7e-ac31-f35fd8543910",
    "SAP Development": "ac2f0447-b1f2-4d7e-bc3e-bf7e9bf377d6",
    "OI - DB Development": None,  # Not found
    "OI - DB Administration": None,  # Not found
    "OI - IBS": "54292b37-54d3-4e43-a406-4732afbfad4d",
    "OI - RDA": "8c63b9c0-21ea-4cb3-b925-f113cc0c31eb",
    "OI - Telecommunications": "og-d9b1de6e-6a08-4039-b1a4-9cb31b025608",
    
    # Additional teams in Jira (for testing)
    "BEST Service": "df299803-b986-4816-866a-78a0845911ad",
    "BITZER Austria": "20790e7a-112d-4cae-85f3-db5e7c53d40e",
    "BITZER BNL": "8bfb12d6-5555-41d5-8e26-30cdb2a5d36d",
    "BITZER France": "82c77270-2405-4705-ad88-6cfdfc021d6b",
    "Bitzer Scroll": "e36ea738-d157-46bc-9506-f093b21e9cc3",
    "BITZER Software": "749f1a46-ea7c-4137-bb3c-866b62580740",
    
    # More teams from Jira
    "IBS - AD/DNS": "3483fec3-d0e9-40d3-abf1-ed0141c1ea80",
    "IBS - AWA": "6809e079-8be3-42f0-b70a-149cc6a4981a",
    "IBS - GPO": "6c1e122d-226f-4630-b7e7-e1d26fadda3f",
    "IBS - SDN": "746fc126-519a-4ee8-a5ce-a5094236f9eb",
    "IBS - SKZ": "dc287682-ce50-4390-8a77-043e58e38403",
    "IBS - SharePoint": "og-401951aa-35fa-4424-8b8f-cb6c0bfa50cd",
    "IBS - File Service": "og-4458f366-9795-4c1d-855e-31dde1d9287e",
    "IBS - Print Service": "og-c1401fc9-5220-4ff4-b539-37a33a071db2",
    "SAP Finance": "8c3b10f7-c102-4d51-8434-4a1b6e1f64da",
    "SAP GDM": "c70e896f-4f03-49ec-8db5-87c7cd8d214a",
    "SAP SMF": "6dc63972-a270-4e80-8367-80e2583fe8da",
}

# ============================================================
# HELPER FUNCTIONS
# ============================================================
auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
headers = {"Accept": "application/json", "Content-Type": "application/json"}


def get_team_id(team_name: str) -> str:
    """Get UUID for team name. Returns None if not found."""
    # Exact match
    if team_name in TEAM_ID_MAP:
        return TEAM_ID_MAP[team_name]
    
    # Case-insensitive match
    for name, uuid in TEAM_ID_MAP.items():
        if name.lower() == team_name.lower():
            return uuid
    
    # Partial match
    team_lower = team_name.lower()
    for name, uuid in TEAM_ID_MAP.items():
        if team_lower in name.lower() or name.lower() in team_lower:
            return uuid
    
    return None


def list_available_teams():
    """List all available teams with their UUIDs"""
    print("=" * 70)
    print("📋 AVAILABLE TEAMS IN JIRA")
    print("=" * 70)
    
    # Teams from trigger_mappings (Excel)
    print("\n🔷 Teams from trigger_mappings (Excel):")
    print("-" * 50)
    trigger_teams = [
        "IBS - CITRIX", "IBS - Virtual Server Infrastructure", "IBS - Mail Service",
        "IBS - Backup", "IBS - ROT", "SAP Basis", "SAP Sales", "SAP Operations",
        "SAP Development", "OI - DB Development", "OI - DB Administration",
        "OI - IBS", "OI - RDA", "OI - Telecommunications"
    ]
    
    for team in trigger_teams:
        uuid = TEAM_ID_MAP.get(team)
        status = "✅" if uuid else "❌ Not in Jira"
        print(f"  {status} {team}")
        if uuid:
            print(f"       ID: {uuid}")
    
    # Additional test teams
    print("\n🔷 Additional teams (for testing):")
    print("-" * 50)
    test_teams = ["BEST Service", "BITZER Austria", "BITZER BNL", "BITZER France", "Bitzer Scroll"]
    for team in test_teams:
        uuid = TEAM_ID_MAP.get(team)
        print(f"  ✅ {team}")
        print(f"       ID: {uuid}")


def create_ticket_with_team(team_name: str, summary: str = None, description: str = None):
    """Create a Jira ticket with team assignment"""
    
    print("=" * 70)
    print("🎫 CREATING JIRA TICKET WITH TEAM ASSIGNMENT")
    print("=" * 70)
    
    team_id = get_team_id(team_name)
    
    if not team_id:
        print(f"\n❌ Team '{team_name}' not found in mapping!")
        print("   Available teams:")
        for name, uuid in TEAM_ID_MAP.items():
            if uuid:
                print(f"     - {name}")
        return None
    
    print(f"\n📋 Team: {team_name}")
    print(f"   UUID: {team_id}")
    
    # Initialize Jira client
    jira = JIRA(
        server=JIRA_BASE_URL,
        basic_auth=(JIRA_EMAIL, JIRA_API_TOKEN)
    )
    
    # Build issue fields
    if not summary:
        summary = f"[TEST] Team Assignment - {team_name}"
    if not description:
        description = f"Test ticket for team assignment.\n\nTeam: {team_name}\nUUID: {team_id}"
    
    issue_fields = {
        'project': {'key': JIRA_PROJECT_KEY},
        'summary': summary,
        'description': description,
        'issuetype': {'name': 'Task'},
        'priority': {'name': 'Medium'},
        TEAM_FIELD_ID: team_id  # Just the UUID string!
    }
    
    try:
        print(f"\n🔄 Creating ticket...")
        issue = jira.create_issue(fields=issue_fields)
        
        print(f"\n✅ SUCCESS!")
        print(f"   Ticket: {issue.key}")
        print(f"   URL: {JIRA_BASE_URL}/browse/{issue.key}")
        print(f"   Team: {team_name}")
        
        return issue.key
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return None


def create_from_email_json(json_file: str, team_name: str):
    """Create Jira ticket from email JSON file with team assignment"""
    
    print("=" * 70)
    print(f"📧 CREATING FROM EMAIL: {json_file}")
    print("=" * 70)
    
    # Load email
    with open(json_file, 'r') as f:
        email = json.load(f)
    
    print(f"\n   Subject: {email.get('subject', 'N/A')[:60]}...")
    print(f"   Priority: {email.get('priority', 'N/A')}")
    print(f"   Trigger: {email.get('trigger_name', 'N/A')}")
    
    # Get team UUID
    team_id = get_team_id(team_name)
    if not team_id:
        print(f"\n❌ Team '{team_name}' not found!")
        return None
    
    print(f"\n   Assigning to: {team_name}")
    print(f"   Team UUID: {team_id}")
    
    # Extract machine name
    machine_name = "Unknown"
    subject = email.get('subject', '')
    body = email.get('body', '')
    
    patterns = [
        r'Machine\s+([A-Za-z0-9]+)\.bitzer',
        r'Computer:\s+([A-Za-z0-9]+)\.',
        r'on\s+([A-Za-z0-9]+)\s+\(',
        r'([A-Z]{2}[A-Z0-9]{3,}[0-9]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, subject + body, re.IGNORECASE)
        if match:
            machine_name = match.group(1).upper()
            break
    
    print(f"   Machine: {machine_name}")
    
    # Build description
    description = f"*Alert Details*\n\n"
    description += f"*Machine:* {machine_name}\n"
    description += f"*Trigger:* {email.get('trigger_name', 'N/A')}\n"
    description += f"*Timestamp:* {email.get('timestamp', 'N/A')}\n"
    description += f"*Priority:* {email.get('priority', 'N/A')}\n"
    description += f"*Team:* {team_name}\n\n"
    description += f"*Email Body:*\n{email.get('body', '')[:1000]}"
    
    # Priority mapping
    priority_map = {"P1": "Highest", "P2": "High", "Informational": "Low"}
    jira_priority = priority_map.get(email.get('priority'), 'Medium')
    
    # Initialize Jira
    jira = JIRA(
        server=JIRA_BASE_URL,
        basic_auth=(JIRA_EMAIL, JIRA_API_TOKEN)
    )
    
    issue_fields = {
        'project': {'key': JIRA_PROJECT_KEY},
        'summary': f"[{email.get('priority', 'NA')}] {email.get('trigger_name', 'Alert')} - {machine_name}",
        'description': description,
        'issuetype': {'name': 'Task'},
        'priority': {'name': jira_priority},
        TEAM_FIELD_ID: team_id
    }
    
    try:
        print(f"\n🔄 Creating ticket...")
        issue = jira.create_issue(fields=issue_fields)
        
        print(f"\n✅ SUCCESS!")
        print(f"   Ticket: {issue.key}")
        print(f"   URL: {JIRA_BASE_URL}/browse/{issue.key}")
        
        return issue.key
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return None


def update_ticket_team(ticket_key: str, team_name: str):
    """Update team on existing ticket"""
    
    team_id = get_team_id(team_name)
    if not team_id:
        print(f"❌ Team '{team_name}' not found!")
        return False
    
    print(f"🔄 Updating {ticket_key} → Team: {team_name}")
    
    r = requests.put(
        f"{JIRA_BASE_URL}/rest/api/3/issue/{ticket_key}",
        auth=auth, headers=headers,
        json={"fields": {TEAM_FIELD_ID: team_id}}
    )
    
    if r.status_code == 204:
        print(f"✅ SUCCESS!")
        return True
    else:
        print(f"❌ Failed: {r.status_code} - {r.text[:100]}")
        return False


def verify_ticket_team(ticket_key: str):
    """Check team value on a ticket"""
    
    r = requests.get(
        f"{JIRA_BASE_URL}/rest/api/3/issue/{ticket_key}",
        auth=auth, headers=headers
    )
    
    if r.status_code != 200:
        print(f"❌ Failed to get ticket: {r.status_code}")
        return
    
    team_value = r.json().get('fields', {}).get(TEAM_FIELD_ID)
    
    print(f"\n📋 Team field in {ticket_key}:")
    if team_value:
        print(json.dumps(team_value, indent=2))
    else:
        print("   (not set)")


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Test Jira Team Assignment")
    
    parser.add_argument('--create', action='store_true', help='Create test ticket')
    parser.add_argument('--json', type=str, help='Create from email JSON file')
    parser.add_argument('--team', type=str, default='BEST Service', help='Team name')
    parser.add_argument('--update', type=str, metavar='TICKET', help='Update existing ticket')
    parser.add_argument('--verify', type=str, metavar='TICKET', help='Verify team on ticket')
    parser.add_argument('--list-teams', action='store_true', help='List available teams')
    
    args = parser.parse_args()
    
    if args.list_teams:
        list_available_teams()
    
    elif args.create:
        create_ticket_with_team(args.team)
    
    elif args.json:
        create_from_email_json(args.json, args.team)
    
    elif args.update:
        update_ticket_team(args.update, args.team)
    
    elif args.verify:
        verify_ticket_team(args.verify)
    
    else:
        print("Quick Test Commands:")
        print("-" * 50)
        print("python test_team_complete.py --list-teams")
        print("python test_team_complete.py --create --team 'BEST Service'")
        print("python test_team_complete.py --create --team 'OI - IBS'")
        print("python test_team_complete.py --json email_01_P2_2025-08-27_10-09-29_00-00.json --team 'SAP Basis'")
        print("python test_team_complete.py --update MAI-1220 --team 'BITZER Austria'")
        print("python test_team_complete.py --verify MAI-1220")


if __name__ == "__main__":
    main()