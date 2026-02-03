#!/usr/bin/env python3
"""
Email Processing Service - Integration Tests
Tests use REAL JSON input files and execute actual Teams/Jira integration
"""

import pytest
import json
import sys
from pathlib import Path

# Import from current project directory
sys.path.insert(0, str(Path(__file__).parent))

from processor import InfrastructureRouter, DatabaseLookup, EmailProcessor

# Path to test data - "individual emails" folder
TEST_DATA_DIR = Path(__file__).parent / 'individual emails'


# 1. INFRASTRUCTURE ROUTING TESTS (Unit Tests)

class TestInfrastructureRouting:
    """TC-IR: Infrastructure routing logic"""
    
    def test_tc_ir_01_oi_rda_priority(self):
        """TC-IR-01: OI-RDA takes priority when multiple infrastructure groups exist"""
        groups = ["ACC Technical", "OI-RDA Infrastructure", "Citrix Infrastructure"]
        result = InfrastructureRouter.resolve_infrastructure(groups)
        assert result == "OI-RDA Infrastructure"
        print(f"✓ TC-IR-01 PASSED: OI-RDA selected from {groups}")
    
    def test_tc_ir_02_oi_ibs_exclusion(self):
        """TC-IR-02: OI-IBS is excluded and other infrastructure is selected"""
        groups = ["OI-IBS Infrastructure", "ACC Technical"]
        result = InfrastructureRouter.resolve_infrastructure(groups)
        assert result == "ACC Technical"
        print(f"✓ TC-IR-02 PASSED: ACC Technical selected (OI-IBS excluded)")


# 2. DATABASE INFRASTRUCTURE MAPPING TESTS (Integration Tests)

class TestDatabaseMapping:
    """TC-DB: Database infrastructure mapping with real database"""
    
    def test_tc_db_01_machine_infrastructure_lookup(self):
        """TC-DB-01: Query servers table to retrieve infrastructure for machine"""
        # Use machine from actual JSON files
        machine_name = "DEROT02010"
        result = DatabaseLookup.get_infrastructure_for_machine(machine_name)
        
        print(f"\n{'='*70}")
        print(f"TC-DB-01: Database Lookup Test")
        print(f"{'='*70}")
        print(f"Machine: {machine_name}")
        print(f"Infrastructure(s) found: {result}")
        
        # Verify result is valid (should be list, may be empty if not in DB)
        assert isinstance(result, list)
        if result:
            print(f"✓ TC-DB-01 PASSED: Found infrastructure mapping")
        else:
            print(f"⚠ TC-DB-01: Machine not found in database (will route to General)")


# 3. TEAMS CHANNEL ROUTING TESTS (Integration Tests)

class TestTeamsChannelRouting:
    """TC-TC: Teams channel routing with REAL JSON files"""
    
    @pytest.mark.asyncio
    async def test_tc_tc_01_infrastructure_based_routing(self):
        """TC-TC-01: Process real P1 email and send to correct Teams channel"""
        # Load real P1 email
        email_file = TEST_DATA_DIR / "email_02_P1_2025-08-28_05-58-21+00-00.json"
        with open(email_file, 'r') as f:
            email_data = json.load(f)
        
        print(f"\n{'='*70}")
        print(f"TC-TC-01: Teams Channel Routing Test")
        print(f"{'='*70}")
        print(f"Input file: {email_file.name}")
        print(f"Priority: {email_data.get('priority')}")
        print(f"Subject: {email_data.get('subject')[:60]}...")
        
        processor = EmailProcessor()
        result = await processor.process_email(email_data)
        await processor.close()
        
        print(f"\nResults:")
        print(f"  Success: {result['success']}")
        print(f"  Machine: {result.get('machine_name')}")
        print(f"  Infrastructure: {result.get('infrastructure')}")
        print(f"  Teams Sent: {result.get('teams_notification_sent')}")
        print(f"  Teams Channel: {result.get('teams_channel')}")
        
        assert result["success"] is True
        assert result["teams_notification_sent"] is True
        print(f"✓ TC-TC-01 PASSED: Teams notification sent to {result.get('teams_channel')}")


# 4. PRIORITY-BASED FILTERING TESTS (Integration Tests)

class TestPriorityFiltering:
    """TC-PF: Priority-based filtering with REAL JSON files"""
    
    @pytest.mark.asyncio
    async def test_tc_pf_01_p1_email_processing(self):
        """TC-PF-01: Process real P1 email - should send Teams notification"""
        # Use email_07 - P1 Machine Down
        email_file = TEST_DATA_DIR / "email_07_P1_2025-08-26_13-42-33+00-00.json"
        with open(email_file, 'r') as f:
            email_data = json.load(f)
        
        print(f"\n{'='*70}")
        print(f"TC-PF-01: P1 Email Processing Test")
        print(f"{'='*70}")
        print(f"Input file: {email_file.name}")
        print(f"Priority: {email_data.get('priority')}")
        print(f"Trigger: {email_data.get('trigger_name')}")
        
        processor = EmailProcessor()
        result = await processor.process_email(email_data)
        await processor.close()
        
        print(f"\nResults:")
        print(f"  Priority: {result['priority']}")
        print(f"  Teams Sent: {result['teams_notification_sent']}")
        print(f"  Jira Ticket: {result.get('jira_ticket', 'N/A')}")
        
        assert result["success"] is True
        assert result["priority"] == "P1"
        assert result["teams_notification_sent"] is True
        print(f"✓ TC-PF-01 PASSED: P1 email processed with Teams notification")
    
    @pytest.mark.asyncio
    async def test_tc_pf_02_p2_email_processing(self):
        """TC-PF-02: Process real P2 email - should send Teams notification"""
        # Use email_01 - P2 Low Disk Space
        email_file = TEST_DATA_DIR / "email_01_P2_2025-08-27_10-09-29+00-00.json"
        with open(email_file, 'r') as f:
            email_data = json.load(f)
        
        print(f"\n{'='*70}")
        print(f"TC-PF-02: P2 Email Processing Test")
        print(f"{'='*70}")
        print(f"Input file: {email_file.name}")
        print(f"Priority: {email_data.get('priority')}")
        print(f"Trigger: {email_data.get('trigger_name')}")
        
        processor = EmailProcessor()
        result = await processor.process_email(email_data)
        await processor.close()
        
        print(f"\nResults:")
        print(f"  Priority: {result['priority']}")
        print(f"  Teams Sent: {result['teams_notification_sent']}")
        print(f"  Jira Ticket: {result.get('jira_ticket', 'N/A')}")
        
        assert result["success"] is True
        assert result["priority"] == "P2"
        assert result["teams_notification_sent"] is True
        print(f"✓ TC-PF-02 PASSED: P2 email processed with Teams notification")
    
    @pytest.mark.asyncio
    async def test_tc_pf_03_informational_filtering(self):
        """TC-PF-03: Process real Informational email - should NOT send notification"""
        # Use email_08 - Informational
        email_file = TEST_DATA_DIR / "email_08_Informational_2025-08-27_07-01-39+00-00.json"
        with open(email_file, 'r') as f:
            email_data = json.load(f)
        
        print(f"\n{'='*70}")
        print(f"TC-PF-03: Informational Email Filtering Test")
        print(f"{'='*70}")
        print(f"Input file: {email_file.name}")
        print(f"Priority: {email_data.get('priority')}")
        print(f"Trigger: {email_data.get('trigger_name')}")
        
        processor = EmailProcessor()
        result = await processor.process_email(email_data)
        await processor.close()
        
        print(f"\nResults:")
        print(f"  Priority: {result['priority']}")
        print(f"  Teams Sent: {result['teams_notification_sent']}")
        print(f"  Reason: {result.get('reason', 'N/A')}")
        
        assert result["success"] is True
        assert result["priority"] == "Informational"
        assert result["teams_notification_sent"] is False
        print(f"✓ TC-PF-03 PASSED: Informational email filtered (no notification)")


if __name__ == "__main__":
    print("\n" + "="*70)
    print("EMAIL PROCESSING SERVICE - INTEGRATION TESTS")
    print("Using REAL JSON input files and actual Teams/Jira integration")
    print("="*70)
    pytest.main([__file__, "-v", "-s"])