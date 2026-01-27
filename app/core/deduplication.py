#!/usr/bin/env python3
"""
Bitzer ControlUp Email Deduplication Script
===========================================

This script processes a batch of ControlUp alert emails (.msg files) and filters out duplicates
based on a combination of trigger_name, computer_name, and subject fields.

Requirements:
- Batch-level deduplication (stateless - no memory between runs)
- Uses simple string concatenation for signature creation
- Processes .msg files from a specified directory
- Integrates with EmailParser and ControlParser components



"""

import os
import sys
import time
from typing import List, Dict, Tuple
from app.core.email_parsing import EmailParser
# from control_parse import EmailData


class BatchDeduplication:
    """
    Batch-level email deduplication for ControlUp alerts.
    
    This class processes a single batch of emails and identifies duplicates
    based on the combination of trigger_name, computer_name, and subject.
    
    Key Features:
    - Stateless operation (no persistence between batches)
    - Simple string concatenation for signature creation
    - Detailed logging of processing results
    """
    
    def __init__(self):
        """Initialize the deduplication engine."""
        self.seen_signatures: set = set()
        self.unique_emails: List[Dict] = []
        self.duplicates: List[Dict] = []
        self.processing_errors: List[Dict] = []
        self.email_parser = EmailParser()
    
    def create_signature(self, fields: Dict) -> str:
        """
        Create a unique signature for deduplication using three key fields.
        
        Args:
            fields: Dictionary containing parsed email fields
            
        Returns:
            String signature for uniqueness comparison
        """
        trigger = fields.get('trigger_name', '').strip()
        computer = fields.get('computer_name', '').strip()
        subject = fields.get('subject', '').strip()
        
        # Use simple string concatenation as per final requirements
        signature = f"{trigger}|{computer}|{subject}"
        return signature
    
    def process_single_email(self, email_dict: Dict) -> Dict:
        """
        Process a single .msg email file.
        
        Args:
            file_path: Path to the .msg file
            
        Returns:
            Dictionary containing processing result and extracted data
        """
        
        try:
            
            
            # Step 2: Parse email content to extract ALL fields
            all_parsed_fields = self.email_parser.parse_email(email_dict)
            
            # Step 3: Get the specific fields needed for deduplication #trigger_name, computer_name, subject,primary_reason
            deduplication_fields = self.email_parser.get_deduplication_fields(email_dict)
            
            # Step 4: Validate that required fields are present for deduplication
            required_fields = ['trigger_name', 'computer_name', 'subject']
            missing_fields = [field for field in required_fields if not deduplication_fields.get(field, '').strip()]
            
            if missing_fields:
                return {
                    'status': 'error',
                    #'file_name': file_name,
                    'error': f'Missing required fields for deduplication: {missing_fields}',
                    'all_parsed_fields': all_parsed_fields,
                    'deduplication_fields': deduplication_fields
                }
            
            return {
                'status': 'success',
                #'file_name': file_name,
                'fields': deduplication_fields,  # For deduplication logic
                'all_parsed_fields': all_parsed_fields,  # ALL fields from email body
                # 'file_path': file_path
            }
            
        except Exception as e:
            return {
                'status': 'error',
                #'file_name': file_name,
                'error': str(e)
            }
    
    def process_batch(self, msg_files: List(Dict)) -> Dict:
        """
        Process all .msg files in the specified directory for deduplication.
        
        Args:
            directory_path: Path to directory containing .msg files
            
        Returns:
            Dictionary containing batch processing results
        """
        print(f"{'='*80}")
        print(f"BITZER CONTROLUP EMAIL DEDUPLICATION")
        print(f"{'='*80}")
        #print(f"Processing directory: {directory_path}")
        print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Find all .msg files in the directory
        #msg_files = []
        
        
        if not msg_files:
            print("⚠️  No .msg files found in the specified directory.")
            return {
                'total_files': 0,
                'processed_successfully': 0,
                'processing_errors': 0,
                'unique_emails': 0,
                'duplicates_filtered': 0,
                'results': []
            }
        
        print(f"Found {len(msg_files)} .msg files to process")
        print(f"{'='*80}")
        
        # Process each email file
        for i in msg_files:
            
            result = self.process_single_email(i)
            
            if result['status'] == 'error':
                print(f"  ❌ Error: {result['error']}")
                self.processing_errors.append(result)
                continue
            
            # Extract fields and create signature
            fields = result['fields']
            signature = self.create_signature(fields)
            
            print(f"  📋 Trigger: {fields['trigger_name'][:50]}...")
            print(f"  💻 Computer: {fields['computer_name']}")
            print(f"  📧 Subject: {fields['subject'][:50]}...")
            
            # Check for duplicates
            if signature in self.seen_signatures:
                print(f"  🔄 DUPLICATE - Same signature already seen in this batch")
                self.duplicates.append({
                    #'file_name': file_name,
                    #'file_path': file_path,
                    'fields': fields,
                    # 'all_parsed_fields': result.get('all_parsed_fields', fields),
                    'all_parsed_fields': result.get('all_parsed_fields', None),
                    'signature': signature
                })
            else:
                print(f"  ✅ UNIQUE - Adding to processing queue")
                self.seen_signatures.add(signature)
                self.unique_emails.append({
                    #'file_name': file_name,
                    #'file_path': file_path,
                    'fields': fields,
                    'all_parsed_fields': result.get('all_parsed_fields', None),
                    'signature': signature
                })
        
        # Generate batch summary
        # return self._generate_batch_summary(len(msg_files))
        return self.unique_emails,self.duplicates
    
    def _generate_batch_summary(self, total_files: int) -> Dict:
        """Generate and display batch processing summary."""
        
        print(f"\n{'='*80}")
        print(f"BATCH PROCESSING SUMMARY")
        print(f"{'='*80}")
        
        summary = {
            'total_files': total_files,
            'processed_successfully': len(self.unique_emails) + len(self.duplicates),
            'processing_errors': len(self.processing_errors),
            'unique_emails': len(self.unique_emails),
            'duplicates_filtered': len(self.duplicates),
            'results': {
                'unique': self.unique_emails,
                'duplicates': self.duplicates,
                'errors': self.processing_errors
            }
        }
        
        print(f"📊 Total .msg files found: {summary['total_files']}")
        print(f"✅ Successfully processed: {summary['processed_successfully']}")
        print(f"❌ Processing errors: {summary['processing_errors']}")
        print(f"🟢 Unique emails (to be processed): {summary['unique_emails']}")
        print(f"🟡 Duplicates filtered: {summary['duplicates_filtered']}")
        
        if summary['unique_emails'] > 0:
            print(f"\n📋 UNIQUE EMAILS TO BE SENT TO NEXT STAGE:")
            for i, email in enumerate(self.unique_emails, 1):
                print(f"  {i:2d}. {email['file_name']}")
                print(f"      🔗 {email['fields']['trigger_name'][:60]}...")
                print(f"      💻 {email['fields']['computer_name']}")
        
        if summary['duplicates_filtered'] > 0:
            print(f"\n🗑️  DUPLICATE EMAILS FILTERED OUT:")
            for i, email in enumerate(self.duplicates, 1):
                print(f"  {i:2d}. {email['file_name']}")
                print(f"      🔗 {email['fields']['trigger_name'][:60]}...")
                print(f"      💻 {email['fields']['computer_name']}")
        
        if summary['processing_errors'] > 0:
            print(f"\n⚠️  PROCESSING ERRORS:")
            for i, error in enumerate(self.processing_errors, 1):
                print(f"  {i:2d}. {error['file_name']}: {error['error']}")
        
        print(f"\n{'='*80}")
        
        return summary
    
    def get_unique_emails_for_processing(self) -> List[Dict]:
        """
        Get the list of unique emails that should be sent to the next stage.
        
        Returns:
            List of dictionaries containing file info and parsed fields
        """
        return self.unique_emails
    
    def get_rabbitmq_payload(self) -> Dict:
        """
        Generate structured payload for RabbitMQ queue.
        
        Returns:
            Dictionary containing batch metadata and unique emails ready for queue
        """
        rabbitmq_payload = {
            "batch_metadata": {
                "batch_id": f"batch_{int(time.time())}",
                "processing_timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "total_files_processed": len(self.unique_emails) + len(self.duplicates) + len(self.processing_errors),
                "unique_emails_count": len(self.unique_emails),
                "duplicates_filtered_count": len(self.duplicates),
                "processing_errors_count": len(self.processing_errors)
            },
            "unique_emails": []
        }
        
        # Add each unique email with ALL extracted fields
        for i, email in enumerate(self.unique_emails):
            # Get ALL parsed fields from the original parsing (not just deduplication fields)
            all_fields = email['all_parsed_fields'] if 'all_parsed_fields' in email else email['fields']
            
            email_payload = {
                "email_id": f"{rabbitmq_payload['batch_metadata']['batch_id']}_email_{i+1:03d}",
                #"source_file": email['file_name'],
                #"source_file_path": email['file_path'],
                "deduplication_signature": email['signature'],
                "deduplication_key_fields": {
                    "trigger_name": email['fields']['trigger_name'],
                    "computer_name": email['fields']['computer_name'],
                    "subject": email['fields']['subject']
                },
                "all_extracted_fields": all_fields  # ALL fields from email body
            }
            rabbitmq_payload["unique_emails"].append(email_payload)
        
        return rabbitmq_payload
    
    def save_results_to_json(self, output_file: str = None) -> str:
        """
        Save processing results to JSON file.
        
        Args:
            output_file: Optional output file path. If None, generates timestamped filename.
            
        Returns:
            Path to the saved JSON file
        """
        if output_file is None:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            output_file = f"deduplication_results_{timestamp}.json"
        
        results = self.get_rabbitmq_payload()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            import json
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        return output_file


def dedup_main(list_emails):
    """
    Main function - Entry point for the deduplication script.
    
    Usage:
    python deduplication_script.py [directory_path]
    
    If no directory is provided, user will be prompted to enter one.
    """
   
    
    # Get directory path from command line or user input
    
    
    try:
        # Initialize the deduplication engine
        deduplicator = BatchDeduplication()
        
        # Process the batch #trigger_name, computer_name, subject
        #{'field': field,'all_parsed_fields': all_parsed_fields,'signature': signature}
        results,duplicated_results = deduplicator.process_batch(list_emails)
        
        return results,duplicated_results
        

        
    except Exception as e:
        print(f"❌ Fatal error during batch processing: {str(e)}")
        sys.exit(1)



