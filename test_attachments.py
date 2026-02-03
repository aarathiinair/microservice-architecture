#!/usr/bin/env python3
"""
Jira Attachment Testing and Debugging Script

This script helps debug and test the attachment functionality for Jira tickets.
It provides multiple modes to understand and fix attachment issues.

Usage:
    python test_attachments.py --mode check          # Check folder structure
    python test_attachments.py --mode test-upload    # Test upload to existing ticket
    python test_attachments.py --mode create-test    # Create test ticket with attachment
    python test_attachments.py --mode api-inspect    # Inspect Jira API responses
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

from jira import JIRA
from config import settings


class AttachmentDebugger:
    """Comprehensive Jira attachment debugging tool"""
    
    def __init__(self):
        self.jira_client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize JIRA client"""
        try:
            print("\n" + "="*70)
            print("🔗 CONNECTING TO JIRA")
            print("="*70)
            print(f"Server: {settings.JIRA_BASE_URL}")
            print(f"Project: {settings.JIRA_PROJECT_KEY}")
            
            self.jira_client = JIRA(
                server=settings.JIRA_BASE_URL,
                basic_auth=(settings.JIRA_EMAIL, settings.JIRA_API_TOKEN)
            )
            print("✅ Jira client initialized successfully\n")
        except Exception as e:
            print(f"❌ Failed to initialize Jira: {e}")
            sys.exit(1)
    
    def check_folder_structure(self):
        """Check for email attachment folders and files"""
        print("\n" + "="*70)
        print("📁 CHECKING FOLDER STRUCTURE")
        print("="*70)
        
        # Folders to check
        folders_to_check = [
            "original emails",      # With space (actual folder)
            "original_emails",      # With underscore (alternative)
            "individual emails",    # With space (actual folder)
            "individual_emails",    # With underscore (alternative)
            "attachments", 
            "emails",
            "."  # Current directory
        ]
        
        found_files = []
        
        for folder_name in folders_to_check:
            folder = Path(folder_name)
            
            if not folder.exists():
                print(f"\n❌ '{folder_name}/' - NOT FOUND")
                continue
            
            print(f"\n✅ '{folder_name}/' - EXISTS")
            
            # Find .msg files
            msg_files = list(folder.glob("*.msg"))
            
            if msg_files:
                print(f"   📎 Found {len(msg_files)} .msg files:")
                for msg_file in sorted(msg_files)[:5]:  # Show first 5
                    size_kb = msg_file.stat().st_size / 1024
                    print(f"      - {msg_file.name} ({size_kb:.1f} KB)")
                    found_files.append(str(msg_file))
                
                if len(msg_files) > 5:
                    print(f"      ... and {len(msg_files) - 5} more files")
            else:
                print(f"   ℹ️  No .msg files in this folder (this is normal for JSON folders)")
        
        print("\n" + "="*70)
        print(f"📊 SUMMARY: Found {len(found_files)} .msg files total")
        
        if not found_files:
            print("\n⚠️  PROBLEM: No .msg files found in any folder!")
            print("\n💡 SOLUTION: ")
            print("   1. Create a folder called 'original emails/' (with space)")
            print("   2. Place your .msg email files in that folder")
            print("   3. Ensure filenames contain machine names (e.g., DESDN01057)")
        
        return found_files
    
    def inspect_existing_ticket(self, ticket_key: str):
        """Inspect an existing Jira ticket to see its attachment structure"""
        print("\n" + "="*70)
        print(f"🔍 INSPECTING TICKET: {ticket_key}")
        print("="*70)
        
        try:
            issue = self.jira_client.issue(ticket_key, expand='attachment')
            
            print(f"\nTicket: {issue.key}")
            print(f"Summary: {issue.fields.summary}")
            print(f"Status: {issue.fields.status}")
            
            # Check attachments
            attachments = issue.fields.attachment
            
            if attachments:
                print(f"\n📎 Attachments ({len(attachments)}):")
                for att in attachments:
                    print(f"\n   Filename: {att.filename}")
                    print(f"   Size: {att.size / 1024:.1f} KB")
                    print(f"   MIME: {att.mimeType}")
                    print(f"   ID: {att.id}")
                    print(f"   Content URL: {att.content}")
                    print(f"   Author: {att.author.displayName}")
                    print(f"   Created: {att.created}")
            else:
                print("\n⚠️  No attachments found on this ticket")
            
            # Show how to replicate with Postman
            print("\n" + "="*70)
            print("📮 POSTMAN REPLICATION")
            print("="*70)
            
            print(f"\n1. GET Request to inspect (what you did):")
            print(f"   URL: {settings.JIRA_BASE_URL}/rest/api/2/issue/{ticket_key}")
            print(f"   Auth: Basic Auth (Email + API Token)")
            
            print(f"\n2. POST Request to add attachment:")
            print(f"   URL: {settings.JIRA_BASE_URL}/rest/api/2/issue/{ticket_key}/attachments")
            print(f"   Headers:")
            print(f"      X-Atlassian-Token: no-check")
            print(f"      Authorization: Basic <your-base64-encoded-credentials>")
            print(f"   Body: form-data")
            print(f"      Key: file")
            print(f"      Value: <select your .msg file>")
            
            return issue
            
        except Exception as e:
            print(f"❌ Error inspecting ticket: {e}")
            return None
    
    def test_upload_to_existing_ticket(self, ticket_key: str, file_path: str):
        """Test uploading an attachment to an existing ticket"""
        print("\n" + "="*70)
        print(f"📤 TESTING UPLOAD TO: {ticket_key}")
        print("="*70)
        
        attachment_file = Path(file_path)
        
        if not attachment_file.exists():
            print(f"❌ File not found: {file_path}")
            return False
        
        print(f"\nFile: {attachment_file.name}")
        print(f"Size: {attachment_file.stat().st_size / 1024:.1f} KB")
        
        try:
            print(f"\n⏳ Uploading...")
            
            with open(attachment_file, 'rb') as f:
                result = self.jira_client.add_attachment(
                    issue=ticket_key,
                    attachment=f,
                    filename=attachment_file.name
                )
            
            print(f"✅ Upload successful!")
            print(f"   Attachment ID: {result.id}")
            print(f"   Filename: {result.filename}")
            print(f"   Size: {result.size / 1024:.1f} KB")
            
            return True
            
        except Exception as e:
            print(f"❌ Upload failed: {e}")
            print(f"\nError details:")
            print(f"   {type(e).__name__}: {str(e)}")
            return False
    
    def create_test_ticket_with_attachment(self, file_path: Optional[str] = None):
        """Create a new test ticket and attach a file"""
        print("\n" + "="*70)
        print("🎫 CREATING TEST TICKET WITH ATTACHMENT")
        print("="*70)
        
        try:
            # Create ticket
            print("\n⏳ Creating ticket...")
            
            issue = self.jira_client.create_issue(
                project=settings.JIRA_PROJECT_KEY,
                summary="[TEST] Attachment Upload Test",
                description="This is a test ticket to verify attachment functionality.\n\n_Created by test_attachments.py_",
                issuetype={'name': settings.JIRA_ISSUE_TYPE},
                priority={'name': 'Low'}
            )
            
            print(f"✅ Ticket created: {issue.key}")
            print(f"   URL: {settings.JIRA_BASE_URL}/browse/{issue.key}")
            
            # Try to attach file if provided
            if file_path:
                attachment_file = Path(file_path)
                
                if attachment_file.exists():
                    print(f"\n⏳ Attaching file: {attachment_file.name}")
                    
                    with open(attachment_file, 'rb') as f:
                        result = self.jira_client.add_attachment(
                            issue=issue.key,
                            attachment=f,
                            filename=attachment_file.name
                        )
                    
                    print(f"✅ Attachment added successfully!")
                    print(f"   Attachment ID: {result.id}")
                else:
                    print(f"\n⚠️  File not found: {file_path}")
            else:
                print(f"\n⚠️  No file specified - ticket created without attachment")
            
            return issue.key
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return None
    
    def compare_api_response(self, ticket_with_attachment: str, ticket_without: str):
        """Compare API responses between tickets with and without attachments"""
        print("\n" + "="*70)
        print("🔬 COMPARING API RESPONSES")
        print("="*70)
        
        try:
            # Get ticket with attachment
            issue_with = self.jira_client.issue(ticket_with_attachment, expand='attachment')
            print(f"\n1. Ticket WITH attachment: {ticket_with_attachment}")
            print(f"   Attachments: {len(issue_with.fields.attachment)}")
            
            if issue_with.fields.attachment:
                att = issue_with.fields.attachment[0]
                print(f"   First attachment:")
                print(f"      Filename: {att.filename}")
                print(f"      Size: {att.size}")
                print(f"      MIME: {att.mimeType}")
            
            # Get ticket without attachment
            issue_without = self.jira_client.issue(ticket_without, expand='attachment')
            print(f"\n2. Ticket WITHOUT attachment: {ticket_without}")
            print(f"   Attachments: {len(issue_without.fields.attachment)}")
            
            # Save responses to JSON files
            with_data = {
                "key": issue_with.key,
                "summary": issue_with.fields.summary,
                "attachment_count": len(issue_with.fields.attachment),
                "attachments": [
                    {
                        "id": att.id,
                        "filename": att.filename,
                        "size": att.size,
                        "mimeType": att.mimeType,
                        "content": att.content
                    }
                    for att in issue_with.fields.attachment
                ]
            }
            
            without_data = {
                "key": issue_without.key,
                "summary": issue_without.fields.summary,
                "attachment_count": len(issue_without.fields.attachment),
                "attachments": []
            }
            
            with open("ticket_with_attachment.json", "w") as f:
                json.dump(with_data, f, indent=2)
            
            with open("ticket_without_attachment.json", "w") as f:
                json.dump(without_data, f, indent=2)
            
            print("\n✅ Saved comparison to:")
            print("   - ticket_with_attachment.json")
            print("   - ticket_without_attachment.json")
            
        except Exception as e:
            print(f"❌ Error: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Jira Attachment Testing and Debugging Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check folder structure for .msg files
  python test_attachments.py --mode check
  
  # Inspect an existing ticket
  python test_attachments.py --mode inspect --ticket MFLP-275
  
  # Test uploading to existing ticket
  python test_attachments.py --mode upload --ticket MFLP-275 --file path/to/file.msg
  
  # Create a new test ticket with attachment
  python test_attachments.py --mode create --file path/to/file.msg
  
  # Compare two tickets (one with, one without attachment)
  python test_attachments.py --mode compare --ticket MFLP-275 --ticket2 MFLP-276
        """
    )
    
    parser.add_argument(
        '--mode',
        required=True,
        choices=['check', 'inspect', 'upload', 'create', 'compare'],
        help='Operation mode'
    )
    
    parser.add_argument(
        '--ticket',
        type=str,
        help='Jira ticket key (e.g., MFLP-275)'
    )
    
    parser.add_argument(
        '--ticket2',
        type=str,
        help='Second Jira ticket key for comparison'
    )
    
    parser.add_argument(
        '--file',
        type=str,
        help='Path to .msg file for upload/create'
    )
    
    args = parser.parse_args()
    
    debugger = AttachmentDebugger()
    
    if args.mode == 'check':
        debugger.check_folder_structure()
    
    elif args.mode == 'inspect':
        if not args.ticket:
            print("❌ --ticket required for inspect mode")
            sys.exit(1)
        debugger.inspect_existing_ticket(args.ticket)
    
    elif args.mode == 'upload':
        if not args.ticket or not args.file:
            print("❌ --ticket and --file required for upload mode")
            sys.exit(1)
        debugger.test_upload_to_existing_ticket(args.ticket, args.file)
    
    elif args.mode == 'create':
        debugger.create_test_ticket_with_attachment(args.file)
    
    elif args.mode == 'compare':
        if not args.ticket or not args.ticket2:
            print("❌ --ticket and --ticket2 required for compare mode")
            sys.exit(1)
        debugger.compare_api_response(args.ticket, args.ticket2)


if __name__ == "__main__":
    main()