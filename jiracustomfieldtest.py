from jira import JIRA
from jira.exceptions import JIRAError
from config import settings
# 1. Configuration
JIRA_SERVER = settings.JIRA_BASE_URL
JIRA_EMAIL = settings.JIRA_EMAIL
JIRA_API_TOKEN = settings.JIRA_API_TOKEN
JIRA_PROJECT_KEY=settings.JIRA_PROJECT_KEY
ISSUE_KEY = 'MAI-648'   # The issue you want to update
CUSTOM_FIELD_ID = "customfield_10001"
TARGET_TEAM_NAME = 'OI - IBS' # The value name you want to select
from jira import JIRA

# Setup
jira = JIRA(server=JIRA_SERVER, basic_auth=(JIRA_EMAIL, JIRA_API_TOKEN))

CUSTOM_FIELD_ID = 'customfield_10001'

print(f"--- Diagnosing {CUSTOM_FIELD_ID} on {ISSUE_KEY} ---")

try:
    # Get edit metadata
    edit_meta = jira.editmeta(ISSUE_KEY)
    fields = edit_meta.get('fields', {})

    if CUSTOM_FIELD_ID not in fields:
        print("🔴 PROBLEM FOUND: The field is MISSING from the Edit Screen.")
        print("   -> Go to Project Settings > Screens > Edit Screen and add this field.")
    else:
        print("🟢 Field is present on the screen.")
        field_data = fields[CUSTOM_FIELD_ID]
        
        # Check Type
        schema = field_data.get('schema', {})
        print(f"   Type: {schema.get('type')} | Custom: {schema.get('custom')}")
        
        # Check Allowed Values
        allowed = field_data.get('allowedValues')
        if allowed is None:
            print("🔴 PROBLEM FOUND: 'allowedValues' is None.")
            print("   -> This usually happens if the field is set to 'Autocomplete' renderer")
            print("      or if it is a Text field, not a Dropdown.")
        else:
            print(f"🟢 Allowed values found: {len(allowed)} items.")

except Exception as e:
    print(f"Error: {e}")
# Try getting values from Create Meta instead of Edit Meta
project_key = 'PROJ' # Your project key
issue_type_name = 'Task' # Your issue type

meta = jira.createmeta(
    projectKeys=project_key, 
    issuetypeNames=issue_type_name, 
    expand='projects.issuetypes.fields'
)

# Parse through the nested JSON to find your field
try:
    # Note: Structure depends on JIRA version (Cloud vs Server)
    # This loop searches for the field in the create metadata
    found = False
    for p in meta['projects']:
        for i in p['issuetypes']:
            if CUSTOM_FIELD_ID in i['fields']:
                field_info = i['fields'][CUSTOM_FIELD_ID]
                allowed = field_info.get('allowedValues', [])
                print(f"Found {len(allowed)} values in CreateMeta")
                for val in allowed:
                    print(val['value'])
                found = True
    
    if not found:
        print("Field still not found in Create Meta.")
        
except Exception as e:
    print(f"Error parsing create meta: {e}")
import json


def main():
    jira = JIRA(server=JIRA_SERVER, basic_auth=(JIRA_EMAIL, JIRA_API_TOKEN))
    
    # ---------------------------------------------------------
    # STEP 1: Find the Team ID using the Teams API
    # The 'Team' field does not hold values; we must ask the Teams service.
    # ---------------------------------------------------------
    print(f"Searching for team: '{TARGET_TEAM_NAME}'...")
    
    # This is the internal endpoint JIRA uses to populate the team picker
    # It works with your existing Basic Auth credentials
    teams_endpoint = f"{JIRA_SERVER}/rest/teams/1.0/teams/find?query={TARGET_TEAM_NAME}"
    
    response = jira._session.get(teams_endpoint)
    
    if response.status_code != 200:
        print(f"Error searching teams: {response.status_code} - {response.text}")
        return

    teams_data = response.json()
    
    # Look for the exact match
    target_team_id = None
    for team in teams_data:
        # The API returns a list of matches (fuzzy search)
        if team.get('title') == TARGET_TEAM_NAME:
            target_team_id = team.get('id')
            print(f"✅ Found Team ID: {target_team_id}")
            break
            
    if not target_team_id:
        print(f"❌ Could not find a team named '{TARGET_TEAM_NAME}'.")
        print("Available partial matches found:", [t.get('title') for t in teams_data])
        return

    # ---------------------------------------------------------
    # STEP 2: Update the Issue
    # For 'Team' fields, the payload is usually just the UUID string or ID
    # ---------------------------------------------------------
    print(f"Updating {ISSUE_KEY}...")
    
    try:
        issue = jira.issue(ISSUE_KEY)
        
        # The payload format for 'atlassian-team' fields is typically just the ID string
        # NOT a dictionary like {'value': 'name'}
        
        # Method A: Direct ID assignment (Most common for this field type)
        # issue.update(fields={CUSTOM_FIELD_ID: target_team_id})
        
        # Method B: If Method A fails, sometimes it requires the 'id' key wrapper
        # Let's try the safest generic update via update() method
        
        issue.update(fields={CUSTOM_FIELD_ID: target_team_id})
        
        print(f"Successfully set Team to '{TARGET_TEAM_NAME}'!")

    except Exception as e:
        print(f"Update failed: {e}")
        print("\n Troubleshooting Tip:")
        print(" If the update failed with a 'complex value' error, try changing the update line to:")
        print(f" issue.update(fields={{'{CUSTOM_FIELD_ID}': {{'id': '{target_team_id}'}} }})")

if __name__ == "__main__":
    main()