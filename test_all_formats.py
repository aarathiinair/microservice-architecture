# test_all_formats.py
import requests
from requests.auth import HTTPBasicAuth
import json

JIRA_BASE_URL = "https://bitzer-sandbox.atlassian.net"
JIRA_EMAIL = "monitoring.ai@bitzer.de"
JIRA_API_TOKEN = 'paste_token_here'
auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
headers = {"Accept": "application/json", "Content-Type": "application/json"}

test_ticket = "MAI-1220"
team_id = "df299803-b986-4816-866a-78a0845911ad"  # BEST Service

formats = [
    ("just UUID string", team_id),
    ("id object", {"id": team_id}),
    ("array with id", [{"id": team_id}]),
    ("array with just UUID", [team_id]),
    ("teamId key", {"teamId": team_id}),
    ("value as UUID", {"value": team_id}),
]

print(f"Testing Team field formats on {test_ticket}\n")

for name, payload in formats:
    print(f"Trying: {name}")
    print(f"  Payload: {json.dumps(payload)}")
    
    r = requests.put(
        f"{JIRA_BASE_URL}/rest/api/3/issue/{test_ticket}",
        auth=auth, headers=headers,
        json={"fields": {"customfield_10001": payload}}
    )
    
    if r.status_code == 204:
        print(f"  ✅ SUCCESS!\n")
        print(f"\nWorking format: {json.dumps(payload)}")
        break
    else:
        error = r.text[:100] if r.text else "No message"
        print(f"  ❌ {r.status_code}: {error}\n")