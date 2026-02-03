
# test_team_id.py
import requests
from requests.auth import HTTPBasicAuth

JIRA_BASE_URL = "https://bitzer-sandbox.atlassian.net"
JIRA_EMAIL = "monitoring.ai@bitzer.de"
JIRA_API_TOKEN = 'paste_token_here'

auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
headers = {"Accept": "application/json", "Content-Type": "application/json"}

# Test with UUID format
test_ticket = "MAI-1220"
team_id = "df299803-b986-4816-866a-78a0845911ad"  # BEST Service

print(f"Testing: Set Team to 'BEST Service' on {test_ticket}")
print(f"Using ID format: {team_id}")

r = requests.put(
    f"{JIRA_BASE_URL}/rest/api/3/issue/{test_ticket}",
    auth=auth, headers=headers,
    json={"fields": {"customfield_10001": {"id": team_id}}}
)

if r.status_code == 204:
    print("✅ SUCCESS! Team field updated.")
    print("\nCorrect format for code:")
    print('  issue.update(fields={"customfield_10001": {"id": "UUID_HERE"}})')
else:
    print(f"❌ Failed: {r.status_code} - {r.text}")