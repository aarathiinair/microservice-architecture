#!/usr/bin/env python3
"""
Test Script: Team Assignment for 14 Teams Channels

Purpose: Test group-based team assignment on existing Jira tickets
- Teams with UUID → Set Team field (customfield_10001)
- Teams without UUID → Log warning, skip (needs to create in Jira)

Usage:
    python test_team_assignment_14.py
"""

from jira import JIRA
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================
JIRA_BASE_URL = "https://bitzer-sandbox.atlassian.net"
JIRA_EMAIL = "monitoring.ai@bitzer.de"
JIRA_API_TOKEN = token = 'paste_token_here'
TEAM_FIELD_ID = "customfield_10001"

# =============================================================================
# TEAM UUID MAPPING
# Team Name (Teams Channel) → Jira Team UUID
# =============================================================================
TEAM_UUID_MAP = {
    # Teams WITH UUID (10 teams - can assign now)
    "IBS - Virtual Server Infrastructure": "be18814d-a872-432f-9d48-aa8a41b61b80",
    "IBS - Mail Service": "og-82d9c204-17c0-46fb-a396-b412a2eb857e",
    "IBS - ROT": "eda8c020-1ee2-490b-bde6-baa2ef36269d",
    "SAP Basis": "cbc86a6e-8c12-4e3a-8ecd-d4c52b83b17b",
    "SAP Sales": "4c652e69-e207-4e98-b4bf-ca90838de87b",
    "SAP Operations": "c066a998-37cd-4f7e-ac31-f35fd8543910",
    "SAP Development": "ac2f0447-b1f2-4d7e-bc3e-bf7e9bf377d6",
    "OI - RDA": "8c63b9c0-21ea-4cb3-b925-f113cc0c31eb",
    "OI - Telecommunications": "og-d9b1de6e-6a08-4039-b1a4-9cb31b025608",
    "OI - IBS": "54292b37-54d3-4e43-a406-4732afbfad4d",
    
    # Teams WITHOUT UUID (4 teams - client needs to create in Jira)
    "IBS - CITRIX": None,
    "IBS - Backup": None,
    "OI - DB Development": None,
    "OI - DB Administration": None,
}

# =============================================================================
# TEST DATA: 14 Teams with Timestamps and Ticket Keys
# Update ticket keys to your existing tickets
# =============================================================================
TEST_CASES = [
    # Teams WITH UUID (should succeed)
    {"team": "IBS - Virtual Server Infrastructure", "timestamp": "2025-12-16 16:45:00", "ticket": "MAI-1153"},
    #{"team": "IBS - Mail Service", "timestamp": "2025-01-07 10:05:00", "ticket": "MAI-1341"},
    #{"team": "IBS - ROT", "timestamp": "2025-01-07 10:10:00", "ticket": "MAI-1342"},
    #{"team": "SAP Basis", "timestamp": "2025-01-07 10:15:00", "ticket": "MAI-1343"},
    #{"team": "SAP Sales", "timestamp": "2025-01-07 10:20:00", "ticket": "MAI-1344"},
    {"team": "SAP Operations", "timestamp": "2026-01-03 22:33:00", "ticket": "MAI-1402"},
    {"team": "SAP Development", "timestamp": "2025-12-17 08:39:00", "ticket": "MAI-1201"},
    {"team": "OI - RDA", "timestamp": "2026-01-04 18:05:00", "ticket": "MAI-1446"},
    #{"team": "OI - Telecommunications", "timestamp": "2025-01-07 10:40:00", "ticket": "MAI-1348"},
    {"team": "OI - IBS", "timestamp": "2026-01-03 22:30:00", "ticket": "MAI-1401"},
    
    # Teams WITHOUT UUID (should warn/skip)
    {"team": "IBS - CITRIX", "timestamp": "2026-01-07 06:16:00", "ticket": "MAI-1456"},
    {"team": "IBS - Backup", "timestamp": "2026-01-05 01:08:00", "ticket": "MAI-1452"},
    #{"team": "OI - DB Development", "timestamp": "2025-01-07 11:00:00", "ticket": "MAI-1352"},
    #{"team": "OI - DB Administration", "timestamp": "2025-01-07 11:05:00", "ticket": "MAI-1353"},
]


def set_team_field(jira_client: JIRA, issue_key: str, team_name: str) -> bool:
    """
    Set Team field on Jira issue.
    
    Args:
        jira_client: JIRA client instance
        issue_key: Jira issue key (e.g., 'MAI-1340')
        team_name: Team name (e.g., 'OI - IBS')
    
    Returns:
        True if successful, False otherwise
    """
    team_id = TEAM_UUID_MAP.get(team_name)
    
    if not team_id:
        return False
    
    try:
        issue = jira_client.issue(issue_key)
        issue.update(fields={TEAM_FIELD_ID: team_id})  # Just UUID string!
        return True
    except Exception as e:
        print(f"      Error: {e}")
        return False


def main():
    print("=" * 70)
    print("🧪 TEST: Team Assignment for 14 Teams Channels")
    print("=" * 70)
    print(f"Jira URL: {JIRA_BASE_URL}")
    print(f"Team Field: {TEAM_FIELD_ID}")
    print(f"Test Cases: {len(TEST_CASES)}")
    print("=" * 70)
    
    # Initialize Jira client
    try:
        jira = JIRA(
            server=JIRA_BASE_URL,
            basic_auth=(JIRA_EMAIL, JIRA_API_TOKEN)
        )
        print("✅ Jira client connected\n")
    except Exception as e:
        print(f"❌ Failed to connect to Jira: {e}")
        return
    
    # Results tracking
    results = {
        "success": [],
        "skipped": [],
        "failed": []
    }
    
    # Process each test case
    for i, test in enumerate(TEST_CASES, 1):
        team = test["team"]
        timestamp = test["timestamp"]
        ticket = test["ticket"]
        
        print(f"[{i:2d}/14] {team}")
        print(f"      Ticket: {ticket}")
        print(f"      Timestamp: {timestamp}")
        
        # Check if team has UUID
        team_id = TEAM_UUID_MAP.get(team)
        
        if team_id is None:
            print(f"      ⚠️  SKIPPED - No UUID ")
            results["skipped"].append({"team": team, "ticket": ticket, "reason": "No UUID"})
        else:
            # Attempt to set team field
            success = set_team_field(jira, ticket, team)
            
            if success:
                print(f"      ✅ SUCCESS - Team field set to '{team}'")
                results["success"].append({"team": team, "ticket": ticket})
            else:
                print(f"      ❌ FAILED - Could not set team field")
                results["failed"].append({"team": team, "ticket": ticket})
        
        print()
    
    # Print summary
    print("=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"✅ Success: {len(results['success'])}")
    print(f"⚠️  Skipped: {len(results['skipped'])}")
    print(f"❌ Failed:  {len(results['failed'])}")
    
    if results["success"]:
        print("\n✅ Successfully assigned teams:")
        for r in results["success"]:
            print(f"   {r['ticket']} → {r['team']}")
    
    if results["skipped"]:
        print("\n⚠️  Skipped (no UUID - action needed):")
        for r in results["skipped"]:
            print(f"   {r['ticket']} → {r['team']}")
    
    if results["failed"]:
        print("\n❌ Failed:")
        for r in results["failed"]:
            print(f"   {r['ticket']} → {r['team']}")
    
    print("\n" + "=" * 70)
    print("🔗 Verify tickets at:")
    for test in TEST_CASES:
        print(f"   {JIRA_BASE_URL}/browse/{test['ticket']}")
    print("=" * 70)


if __name__ == "__main__":
    main()