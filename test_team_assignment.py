#!/usr/bin/env python3
"""
Quick Test Script: Jira Team Field Assignment
Tests setting the Team custom field on Jira tickets

Usage:
    python test_team_assignment.py --discover           # Find Team field ID
    python test_team_assignment.py --test-update MAI-1139   # Test update existing ticket
    python test_team_assignment.py --create             # Create ticket with team assignment
    python test_team_assignment.py --create --team "Bitzer Austria"  # Specific team
"""

import argparse
import json
import requests
from requests.auth import HTTPBasicAuth
from jira import JIRA

# ============================================================
# CONFIGURATION - Update these values
# ============================================================
JIRA_BASE_URL = "https://bitzer-sandbox.atlassian.net"
JIRA_EMAIL = "monitoring.ai@bitzer.de"
JIRA_API_TOKEN = "paste_token_here"
JIRA_PROJECT_KEY = "MAI"

# Team field ID - SET THIS AFTER RUNNING --discover
TEAM_FIELD_ID = "customfield_10001"

# Available teams in Jira (temporary - will be updated by client)
AVAILABLE_TEAMS = [
    "Best Service",
    "Bitzer Austria", 
    "Bitzer BNL",
    "Bitzer France",
    "Bitzer Scroll"
]

# ============================================================
# API HELPER FUNCTIONS
# ============================================================
auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
headers = {"Accept": "application/json", "Content-Type": "application/json"}


def discover_team_field():
    """Step 1: Discover the Team custom field ID"""
    print("=" * 70)
    print("🔍 DISCOVERING TEAM CUSTOM FIELD")
    print("=" * 70)
    
    # Get all fields
    response = requests.get(
        f"{JIRA_BASE_URL}/rest/api/3/field",
        auth=auth,
        headers=headers
    )
    
    if response.status_code != 200:
        print(f"❌ Failed to get fields: {response.status_code}")
        print(response.text)
        return None
    
    fields = response.json()
    team_fields = []
    
    print("\n📋 Looking for 'Team' related fields...\n")
    
    for field in fields:
        field_name = field.get('name', '').lower()
        if 'team' in field_name:
            field_info = {
                'name': field['name'],
                'id': field['id'],
                'custom': field.get('custom', False),
                'schema_type': field.get('schema', {}).get('type', 'N/A')
            }
            team_fields.append(field_info)
            
            custom_marker = "✅ CUSTOM" if field_info['custom'] else "⚪ Built-in"
            print(f"{custom_marker}")
            print(f"   Name: {field_info['name']}")
            print(f"   ID:   {field_info['id']}")
            print(f"   Type: {field_info['schema_type']}")
            print()
    
    if not team_fields:
        print("❌ No team-related fields found!")
        return None
    
    # Find the most likely candidate (custom field named "Team")
    for f in team_fields:
        if f['name'].lower() == 'team' and f['custom']:
            print("=" * 70)
            print(f"🎯 RECOMMENDED: Use '{f['id']}' for Team field")
            print("=" * 70)
            print(f"\nUpdate the script: TEAM_FIELD_ID = \"{f['id']}\"")
            return f['id']
    
    return team_fields[0]['id'] if team_fields else None


def check_field_from_ticket(ticket_key: str = "MAI-1139"):
    """Check the Team field structure from an existing ticket"""
    print(f"\n📖 Checking Team field in ticket {ticket_key}...")
    
    response = requests.get(
        f"{JIRA_BASE_URL}/rest/api/3/issue/{ticket_key}",
        auth=auth,
        headers=headers
    )
    
    if response.status_code != 200:
        print(f"❌ Failed to get ticket: {response.status_code}")
        return
    
    fields = response.json().get('fields', {})
    
    # Look for team-related custom fields
    print("\n📋 Custom fields containing 'team' in this ticket:\n")
    
    for field_id, value in fields.items():
        if field_id.startswith('customfield_'):
            if value is not None:
                # Check if it looks like a team field
                if isinstance(value, dict) and ('value' in value or 'name' in value):
                    print(f"   {field_id}: {json.dumps(value, indent=2)}")
                elif isinstance(value, str) and any(t.lower() in value.lower() for t in ['service', 'bitzer', 'team']):
                    print(f"   {field_id}: {value}")


def get_allowed_team_values(field_id: str):
    """Get allowed values for the Team field"""
    print(f"\n📋 Getting allowed values for {field_id}...")
    
    # Method 1: Try getting field context
    response = requests.get(
        f"{JIRA_BASE_URL}/rest/api/3/field/{field_id}/context",
        auth=auth,
        headers=headers
    )
    
    if response.status_code == 200:
        print("Field contexts:")
        print(json.dumps(response.json(), indent=2))
    
    # Method 2: Get from create metadata
    response = requests.get(
        f"{JIRA_BASE_URL}/rest/api/3/issue/createmeta/{JIRA_PROJECT_KEY}/issuetypes",
        auth=auth,
        headers=headers
    )
    
    if response.status_code == 200:
        issue_types = response.json().get('issueTypes', [])
        for it in issue_types:
            if it.get('name') == 'Task':
                print(f"\nIssue type: Task (ID: {it.get('id')})")
                
                # Get fields for this issue type
                fields_response = requests.get(
                    f"{JIRA_BASE_URL}/rest/api/3/issue/createmeta/{JIRA_PROJECT_KEY}/issuetypes/{it.get('id')}",
                    auth=auth,
                    headers=headers
                )
                
                if fields_response.status_code == 200:
                    fields_data = fields_response.json()
                    team_field = fields_data.get('fields', {}).get(field_id)
                    if team_field:
                        print(f"\nTeam field metadata:")
                        print(json.dumps(team_field, indent=2))
                        
                        allowed = team_field.get('allowedValues', [])
                        if allowed:
                            print(f"\n✅ Allowed values:")
                            for val in allowed:
                                print(f"   - {val.get('value', val.get('name', val))}")
                            return allowed
    
    return []


def test_update_team_field(ticket_key: str, team_name: str):
    """Test updating the Team field on an existing ticket"""
    
    if not TEAM_FIELD_ID:
        print("❌ TEAM_FIELD_ID not set! Run --discover first.")
        return False
    
    print(f"\n🔄 Testing Team field update on {ticket_key}")
    print(f"   Team: {team_name}")
    print(f"   Field ID: {TEAM_FIELD_ID}")
    
    # Try different formats based on field type
    formats_to_try = [
        ("single-select (object)", {TEAM_FIELD_ID: {'value': team_name}}),
        ("single-select (id)", {TEAM_FIELD_ID: {'id': team_name}}),
        ("text", {TEAM_FIELD_ID: team_name}),
    ]
    
    for format_name, payload in formats_to_try:
        print(f"\n   Trying format: {format_name}...")
        
        response = requests.put(
            f"{JIRA_BASE_URL}/rest/api/3/issue/{ticket_key}",
            auth=auth,
            headers=headers,
            json={"fields": payload}
        )
        
        if response.status_code == 204:
            print(f"   ✅ SUCCESS with {format_name}!")
            print(f"\n   📝 Use this format in code:")
            print(f"      issue.update(fields={{{TEAM_FIELD_ID}: {payload[TEAM_FIELD_ID]!r}}})")
            return True
        else:
            error_text = response.text[:200] if response.text else "No error message"
            print(f"   ❌ Failed ({response.status_code}): {error_text}")
    
    return False


def create_ticket_with_team(team_name: str = "Best Service"):
    """Create a new Jira ticket and set the Team field"""
    
    print("=" * 70)
    print("🎫 CREATING JIRA TICKET WITH TEAM ASSIGNMENT")
    print("=" * 70)
    
    # Initialize Jira client
    jira = JIRA(
        server=JIRA_BASE_URL,
        basic_auth=(JIRA_EMAIL, JIRA_API_TOKEN)
    )
    
    # Build issue fields
    issue_fields = {
        'project': {'key': JIRA_PROJECT_KEY},
        'summary': f"[TEST] Team Assignment Test - {team_name}",
        'description': f"This is a test ticket to verify Team field assignment.\n\nTeam: {team_name}",
        'issuetype': {'name': 'Task'},
        'priority': {'name': 'Medium'}
    }
    
    # Add Team field if configured
    if TEAM_FIELD_ID:
        # Format for single-select field
        issue_fields[TEAM_FIELD_ID] = {'value': team_name}
        print(f"\n📋 Creating ticket with Team: {team_name}")
    else:
        print("\n⚠️  TEAM_FIELD_ID not set - creating ticket without team assignment")
        print("   Run --discover first to find the field ID")
    
    try:
        # Create the ticket
        print(f"\n🔄 Creating ticket...")
        issue = jira.create_issue(fields=issue_fields)
        
        print(f"\n✅ Ticket created: {issue.key}")
        print(f"   URL: {JIRA_BASE_URL}/browse/{issue.key}")
        
        # Verify the Team field was set
        if TEAM_FIELD_ID:
            created_issue = jira.issue(issue.key)
            team_value = getattr(created_issue.fields, TEAM_FIELD_ID, None)
            
            if team_value:
                if hasattr(team_value, 'value'):
                    print(f"   Team field value: {team_value.value}")
                else:
                    print(f"   Team field value: {team_value}")
            else:
                print("   ⚠️ Team field appears empty - may need different format")
        
        return issue.key
        
    except Exception as e:
        print(f"\n❌ Error creating ticket: {e}")
        
        # If it failed because of Team field format, try without it
        if TEAM_FIELD_ID and TEAM_FIELD_ID in str(e):
            print("\n🔄 Retrying without Team field...")
            del issue_fields[TEAM_FIELD_ID]
            
            try:
                issue = jira.create_issue(fields=issue_fields)
                print(f"\n✅ Ticket created (without team): {issue.key}")
                print(f"   URL: {JIRA_BASE_URL}/browse/{issue.key}")
                
                # Now try to update the team field separately
                print(f"\n🔄 Attempting to set Team field via update...")
                test_update_team_field(issue.key, team_name)
                
                return issue.key
            except Exception as e2:
                print(f"❌ Still failed: {e2}")
        
        return None


def create_ticket_from_json(json_file: str, team_name: str = None):
    """Create Jira ticket from email JSON file"""
    
    print("=" * 70)
    print(f"📧 PROCESSING EMAIL: {json_file}")
    print("=" * 70)
    
    # Load email data
    with open(json_file, 'r') as f:
        email_data = json.load(f)
    
    print(f"\n   Subject: {email_data.get('subject', 'N/A')[:60]}...")
    print(f"   Priority: {email_data.get('priority', 'N/A')}")
    print(f"   Trigger: {email_data.get('trigger_name', 'N/A')}")
    
    # Use provided team or pick from available
    if not team_name:
        team_name = AVAILABLE_TEAMS[0]  # Default to first team
    
    print(f"\n   Assigning to Team: {team_name}")
    
    # Initialize Jira client
    jira = JIRA(
        server=JIRA_BASE_URL,
        basic_auth=(JIRA_EMAIL, JIRA_API_TOKEN)
    )
    
    # Extract machine name
    import re
    machine_name = "Unknown"
    subject = email_data.get('subject', '')
    body = email_data.get('body', '')
    
    patterns = [
        r'Machine\s+([A-Za-z0-9]+)\.bitzer',
        r'on\s+([A-Za-z0-9]+)\s+\(',
        r'([A-Z]{2}[A-Z0-9]{3,}[0-9]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, subject + body, re.IGNORECASE)
        if match:
            machine_name = match.group(1).upper()
            break
    
    # Build description
    description = f"*Alert Details*\n\n"
    description += f"*Machine:* {machine_name}\n"
    description += f"*Trigger:* {email_data.get('trigger_name', 'N/A')}\n"
    description += f"*Timestamp:* {email_data.get('timestamp', 'N/A')}\n"
    description += f"*Priority:* {email_data.get('priority', 'N/A')}\n\n"
    description += f"*Email Body:*\n{email_data.get('body', '')[:1000]}"
    
    # Priority mapping
    priority_map = {
        "P1": "Highest",
        "P2": "High",
        "Informational": "Low",
        "NA": "Lowest"
    }
    jira_priority = priority_map.get(email_data.get('priority'), 'Medium')
    
    # Build issue fields
    issue_fields = {
        'project': {'key': JIRA_PROJECT_KEY},
        'summary': f"[{email_data.get('priority', 'NA')}] {email_data.get('trigger_name', 'Alert')} - {machine_name}",
        'description': description,
        'issuetype': {'name': 'Task'},
        'priority': {'name': jira_priority}
    }
    
    # Add Team field if configured
    if TEAM_FIELD_ID:
        issue_fields[TEAM_FIELD_ID] = {'value': team_name}
    
    try:
        print(f"\n🔄 Creating Jira ticket...")
        issue = jira.create_issue(fields=issue_fields)
        
        print(f"\n✅ Ticket created: {issue.key}")
        print(f"   URL: {JIRA_BASE_URL}/browse/{issue.key}")
        print(f"   Team: {team_name}")
        
        return issue.key
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return None


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="Test Jira Team Field Assignment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_team_assignment.py --discover
  python test_team_assignment.py --test-update MAI-1139
  python test_team_assignment.py --create
  python test_team_assignment.py --create --team "Bitzer Austria"
  python test_team_assignment.py --json email_01.json --team "Best Service"
        """
    )
    
    parser.add_argument('--discover', action='store_true',
                        help='Discover Team custom field ID')
    parser.add_argument('--test-update', type=str, metavar='TICKET',
                        help='Test updating Team field on existing ticket')
    parser.add_argument('--create', action='store_true',
                        help='Create a test ticket with team assignment')
    parser.add_argument('--json', type=str, metavar='FILE',
                        help='Create ticket from email JSON file')
    parser.add_argument('--team', type=str, default='Best Service',
                        help='Team name to assign (default: Best Service)')
    parser.add_argument('--check-ticket', type=str, metavar='TICKET',
                        help='Check Team field value in existing ticket')
    
    args = parser.parse_args()
    
    if args.discover:
        field_id = discover_team_field()
        if field_id:
            check_field_from_ticket()
            get_allowed_team_values(field_id)
    
    elif args.test_update:
        if not TEAM_FIELD_ID:
            print("⚠️  TEAM_FIELD_ID not set!")
            print("   Run --discover first, then update TEAM_FIELD_ID in the script.")
            return
        test_update_team_field(args.test_update, args.team)
    
    elif args.create:
        create_ticket_with_team(args.team)
    
    elif args.json:
        create_ticket_from_json(args.json, args.team)
    
    elif args.check_ticket:
        check_field_from_ticket(args.check_ticket)
    
    else:
        # Default: run discovery
        print("No action specified. Running discovery...\n")
        discover_team_field()
        
        print("\n" + "=" * 70)
        print("📋 AVAILABLE TEAMS (in Jira - temporary)")
        print("=" * 70)
        for i, team in enumerate(AVAILABLE_TEAMS, 1):
            print(f"   {i}. {team}")
        
        print("\n" + "=" * 70)
        print("📋 QUICK COMMANDS")
        print("=" * 70)
        print("1. Find Team field ID:")
        print("   python test_team_assignment.py --discover")
        print("\n2. Test update on existing ticket:")
        print("   python test_team_assignment.py --test-update MAI-1139 --team 'Best Service'")
        print("\n3. Create new test ticket:")
        print("   python test_team_assignment.py --create --team 'Bitzer Austria'")
        print("\n4. Create from email JSON:")
        print("   python test_team_assignment.py --json email_01_P2_2025-08-27_10-09-29_00-00.json --team 'Best Service'")


if __name__ == "__main__":
    main()