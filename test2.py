from jira import JIRA
import json
from config import settings
JIRA_SERVER = settings.JIRA_BASE_URL
JIRA_EMAIL = settings.JIRA_EMAIL
JIRA_API_TOKEN = settings.JIRA_API_TOKEN
JIRA_PROJECT_KEY=settings.JIRA_PROJECT_KEY
ISSUE_KEY = 'MAI-648'   # The issue you want to update
CUSTOM_FIELD_ID = "customfield_10001"
TARGET_TEAM_NAME = 'OI - IBS'
CUSTOM_FIELD_ID = "customfield_10001"
  # Team field

def get_all_teams(jira, query=""):
    """
    Get all teams from JIRA using the Teams API.
    This is specific to atlassian-team custom field type.
    
    Args:
        jira: JIRA client instance
        query: Optional search query to filter teams (empty returns all)
    
    Returns:
        list: List of team dictionaries with id, title, and other metadata
    """
    try:
        # The Teams API endpoint - this is what JIRA uses internally
        # for the atlassian-team field type
        teams_endpoint = f"{JIRA_SERVER}/rest/teams/1.0/teams/find"
        
        params = {}
        if query:
            params['query'] = query
        
        print(f"Querying Teams API...")
        if query:
            print(f"Search query: '{query}'")
        else:
            print(f"Retrieving all teams (no filter)")
        
        response = jira._session.get(teams_endpoint, params=params)
        
        if response.status_code != 200:
            print(f"❌ Teams API error: {response.status_code}")
            print(f"Response: {response.text}")
            return []
        
        teams_data = response.json()
        
        if not teams_data:
            print("❌ No teams found")
            return []
        
        print(f"\n✅ Found {len(teams_data)} team(s):")
        print("="*70)
        
        teams = []
        for idx, team in enumerate(teams_data, 1):
            team_info = {
                'id': team.get('id'),
                'title': team.get('title'),
                'description': team.get('description', ''),
                'avatarUrl': team.get('avatarUrl', ''),
                'membersCount': team.get('membersCount', 0)
            }
            teams.append(team_info)
            
            print(f"\n{idx}. Team: {team_info['title']}")
            print(f"   ID: {team_info['id']}")
            if team_info['description']:
                print(f"   Description: {team_info['description']}")
            print(f"   Members: {team_info['membersCount']}")
        
        print("="*70)
        return teams
        
    except Exception as e:
        print(f"❌ Error retrieving teams: {e}")
        import traceback
        traceback.print_exc()
        return []


def get_team_by_name(jira, team_name):
    """
    Find a specific team by exact name match.
    
    Args:
        jira: JIRA client instance
        team_name: Exact team name to search for
    
    Returns:
        dict: Team information or None if not found
    """
    print(f"Searching for team: '{team_name}'...")
    
    # Search with the team name
    teams = get_all_teams(jira, query=team_name)
    
    # Find exact match
    for team in teams:
        if team['title'] == team_name:
            print(f"\n✅ Found exact match!")
            print(f"   Team: {team['title']}")
            print(f"   ID: {team['id']}")
            return team
    
    # If no exact match, show what we found
    if teams:
        print(f"\n⚠️ No exact match for '{team_name}'")
        print("Available partial matches:")
        for team in teams:
            print(f"  - {team['title']}")
    else:
        print(f"\n❌ No teams found matching '{team_name}'")
    
    return None


def update_team_field(jira, issue_key, custom_field_id, team_id):
    """
    Update the Team field with a team ID.
    
    Based on the XML structure, the field expects just the team ID (UUID).
    
    Args:
        jira: JIRA client instance
        issue_key: Issue key (e.g., 'PROJ-123')
        custom_field_id: Custom field ID (e.g., 'customfield_10001')
        team_id: Team UUID (e.g., '54292b37-54d3-4e43-a406-4732afbfad4d')
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        issue = jira.issue(issue_key)
        
        print(f"\nUpdating {issue_key}...")
        print(f"Team ID: {team_id}")
        
        # Format 1: Direct ID assignment (most common for atlassian-team)
        # Based on your XML: <customfieldvalue id="...">Team Name</customfieldvalue>
        # The field expects just the UUID string
        try:
            issue.update(fields={custom_field_id: team_id})
            print(f"✅ Successfully updated with direct ID format")
            return True
        except Exception as e1:
            print(f"Format 1 (direct ID) failed: {e1}")
        
        # Format 2: ID wrapper object
        try:
            issue.update(fields={custom_field_id: {'id': team_id}})
            print(f"✅ Successfully updated with ID wrapper format")
            return True
        except Exception as e2:
            print(f"Format 2 (ID wrapper) failed: {e2}")
        
        # Format 3: Value object (less common for team fields)
        try:
            issue.update(fields={custom_field_id: {'value': team_id}})
            print(f"✅ Successfully updated with value object format")
            return True
        except Exception as e3:
            print(f"Format 3 (value object) failed: {e3}")
        
        print(f"\n❌ All update formats failed")
        return False
        
    except Exception as e:
        print(f"❌ Update error: {e}")
        import traceback
        traceback.print_exc()
        return False


def list_and_update(jira, issue_key, custom_field_id, team_name):
    """
    Complete workflow: Find team by name and update issue.
    
    Args:
        jira: JIRA client instance
        issue_key: Issue key to update
        custom_field_id: Team field custom field ID
        team_name: Name of the team to set
    
    Returns:
        bool: True if successful, False otherwise
    """
    print("="*70)
    print("STEP 1: Finding Team")
    print("="*70)
    
    # Find the team
    team = get_team_by_name(jira, team_name)
    
    if not team:
        print(f"\n❌ Cannot proceed: Team '{team_name}' not found")
        return False
    
    print("\n" + "="*70)
    print("STEP 2: Updating Issue")
    print("="*70)
    
    # Update the issue
    success = update_team_field(jira, issue_key, custom_field_id, team['id'])
    
    if success:
        print(f"\n🎉 Successfully set Team to '{team_name}' on {issue_key}")
    
    return success


def get_current_team(jira, issue_key, custom_field_id):
    """
    Get the current team value from an issue.
    
    Args:
        jira: JIRA client instance
        issue_key: Issue key
        custom_field_id: Team field custom field ID
    
    Returns:
        dict: Current team info or None
    """
    try:
        issue = jira.issue(issue_key)
        
        # Get the raw field value
        team_value = getattr(issue.fields, custom_field_id, None)
        
        if team_value:
            print(f"\nCurrent team on {issue_key}:")
            print(f"  Raw value: {team_value}")
            print(f"  Type: {type(team_value)}")
            
            # The field might return different structures
            if isinstance(team_value, str):
                # Just the ID
                return {'id': team_value}
            elif isinstance(team_value, dict):
                # Object with id and possibly name
                return team_value
            else:
                print(f"  Unexpected type: {type(team_value)}")
                return {'raw': str(team_value)}
        else:
            print(f"\n{issue_key} has no team set")
            return None
            
    except Exception as e:
        print(f"❌ Error reading current team: {e}")
        return None


def main():
    # Initialize JIRA connection
    jira = JIRA(server=JIRA_SERVER, basic_auth=(JIRA_EMAIL, JIRA_API_TOKEN))
    
    print(f"Connected to JIRA: {JIRA_SERVER}\n")
    
    # ===================================================================
    # Example 1: List all teams (or search for specific teams)
    # ===================================================================
    print("EXAMPLE 1: List all available teams")
    print("="*70)
    
    all_teams = get_all_teams(jira, query="")  # Empty query = all teams
    # Or search for specific teams:
    # teams = get_all_teams(jira, query="OI")  # Search for teams with "OI"
    
    # ===================================================================
    # Example 2: Find a specific team by exact name
    # ===================================================================
    print("\n\nEXAMPLE 2: Find team by exact name")
    print("="*70)
    
    target_team_name = "OI - IBS"  # From your XML example
    team = get_team_by_name(jira, target_team_name)
    
    if team:
        print(f"\nTeam Details:")
        print(f"  Name: {team['title']}")
        print(f"  ID: {team['id']}")
    
    # ===================================================================
    # Example 3: Update an issue with a team
    # ===================================================================
    # Uncomment to update an issue:
    
    # issue_key = "PROJ-123"  # Replace with your issue key
    # team_name = "OI - IBS"  # Replace with your team name
    # 
    # success = list_and_update(jira, issue_key, CUSTOM_FIELD_ID, team_name)
    
    # ===================================================================
    # Example 4: Check current team on an issue
    # ===================================================================
    # Uncomment to check current team:
    
    # issue_key = "PROJ-123"
    # current = get_current_team(jira, issue_key, CUSTOM_FIELD_ID)


if __name__ == "__main__":
    main()