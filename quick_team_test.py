# quick_team_test.py
import requests
from requests.auth import HTTPBasicAuth
import json

JIRA_BASE_URL = "https://bitzer-sandbox.atlassian.net"
JIRA_EMAIL = "monitoring.ai@bitzer.de"
JIRA_API_TOKEN = "paste_token_here"
TEAM_FIELD_ID = "customfield_10001"

auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
headers = {"Accept": "application/json", "Content-Type": "application/json"}

# Step 1: Check current value in a ticket that HAS a team set
print("=" * 60)
print("Step 1: Check Team field in existing ticket")
print("=" * 60)

r = requests.get(f"{JIRA_BASE_URL}/rest/api/3/issue/MAI-1139", auth=auth, headers=headers)
team_value = r.json().get('fields', {}).get(TEAM_FIELD_ID)
print(f"\nTeam field (customfield_10001) in MAI-1139:")
print(json.dumps(team_value, indent=2))

# Step 2: Get all available teams in Jira
print("\n" + "=" * 60)
print("Step 2: Get available Atlassian Teams")
print("=" * 60)

# Try the teams API
r = requests.get(f"{JIRA_BASE_URL}/rest/api/3/teams/search", auth=auth, headers=headers)
if r.status_code == 200:
    teams = r.json()
    print("\nAvailable teams:")
    print(json.dumps(teams, indent=2))
else:
    print(f"Teams API not available ({r.status_code})")
    
    # Alternative: check team picker options
    r = requests.get(
        f"{JIRA_BASE_URL}/rest/api/3/jql/autocompletedata/suggestions?fieldName=Team",
        auth=auth, headers=headers
    )
    if r.status_code == 200:
        print("\nTeam suggestions:")
        print(json.dumps(r.json(), indent=2))

# Step 3: Test updating the field with different formats
print("\n" + "=" * 60)
print("Step 3: Test updating Team field on MAI-1220")
print("=" * 60)

test_ticket = "MAI-1220"  # Use a recent test ticket

formats = [
    ("name string", {TEAM_FIELD_ID: "Best Service"}),
    ("value object", {TEAM_FIELD_ID: {"value": "Best Service"}}),
    ("name object", {TEAM_FIELD_ID: {"name": "Best Service"}}),
]

for fmt_name, payload in formats:
    print(f"\nTrying: {fmt_name}")
    print(f"Payload: {json.dumps(payload)}")
    
    r = requests.put(
        f"{JIRA_BASE_URL}/rest/api/3/issue/{test_ticket}",
        auth=auth, headers=headers,
        json={"fields": payload}
    )
    
    if r.status_code == 204:
        print(f"✅ SUCCESS!")
        break
    else:
        print(f"❌ Failed: {r.status_code} - {r.text[:150]}")