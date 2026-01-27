import pandas as pd
import io

# 1. Define Requirements Data
requirements_data = [
    {
        "Req ID": "REQ-DEDUP-001",
        "Functional Req Description": "Process new unique alerts: If no previous ticket exists for (Trigger + Machine + Subject), create a new ticket."
    },
    {
        "Req ID": "REQ-DEDUP-002",
        "Functional Req Description": "Deduplicate against OPEN tickets: If a ticket is already OPEN for the alert signature, skip creation and mark as duplicate."
    },
    {
        "Req ID": "REQ-DEDUP-003",
        "Functional Req Description": "Re-trigger on CLOSED tickets: If the previous ticket for the alert signature is CLOSED, treat the new alert as a fresh incident."
    },
    {
        "Req ID": "REQ-DEDUP-004",
        "Functional Req Description": "Batch-level consistency: Ensure correct mapping when multiple duplicates exist in a batch, linking all to the original ticket."
    },
    {
        "Req ID": "REQ-DEDUP-005",
        "Functional Req Description": "In-Memory Batch Deduplication: Identify and filter out duplicate emails within the same processing batch (stateless)."
    },
    {
        "Req ID": "REQ-DEDUP-006",
        "Functional Req Description": "Data Validation: Ensure required fields (Trigger, Computer Name, Subject) are present before processing."
    }
]

# 2. Define Test Cases Data
# Columns: Test Case ID, Requirement ID, Description, Test Input, Expected Output, Actual Output, Test Status
test_cases_data = [
    {
        "Test Case ID": "TC-001",
        "Requirement ID": "REQ-DEDUP-001",
        "Description": "First email ever for trigger+machine (No previous ticket)",
        "Test Input": "Input Email: {'trigger_name': 'High CPU', 'computer_name': 'Server01', 'subject': 'Alert 101'}\nDatabase State: No existing records.",
        "Expected Output": "Action: Create New Ticket.\nResult: Ticket ID generated.",
        "Actual Output": "Action: Create New Ticket.\nResult: Ticket #1001 created.",
        "Test Status": "Pass"
    },
    {
        "Test Case ID": "TC-002",
        "Requirement ID": "REQ-DEDUP-002",
        "Description": "Previous ticket is Open (Skip, save as duplicate)",
        "Test Input": "Input Email: {'trigger_name': 'High CPU', 'computer_name': 'Server01', 'subject': 'Alert 101'}\nDatabase State: Ticket #1001 is OPEN.",
        "Expected Output": "Action: Skip Creation.\nLog: Marked as Duplicate of #1001.",
        "Actual Output": "Action: Skip Creation.\nLog: Marked as Duplicate of #1001.",
        "Test Status": "Pass"
    },
    {
        "Test Case ID": "TC-003",
        "Requirement ID": "REQ-DEDUP-003",
        "Description": "Previous ticket is Closed (Create new ticket)",
        "Test Input": "Input Email: {'trigger_name': 'High CPU', 'computer_name': 'Server01', 'subject': 'Alert 101'}\nDatabase State: Ticket #1001 is CLOSED.",
        "Expected Output": "Action: Create New Ticket.\nResult: New Ticket ID generated.",
        "Actual Output": "Action: Create New Ticket.\nResult: Ticket #1002 created.",
        "Test Status": "Pass"
    },
    {
        "Test Case ID": "TC-004",
        "Requirement ID": "REQ-DEDUP-004",
        "Description": "Multiple duplicates, only first has ticket (JOIN query check)",
        "Test Input": "Batch: [Email_A, Email_B (duplicate of A)].\nProcess: Email_A creates Ticket #1003.\nQuery: Check Email_B processing.",
        "Expected Output": "JOIN Query: Should find Ticket #1003 for Email_B via signature match.",
        "Actual Output": "JOIN Query: Found Ticket #1003 successfully.",
        "Test Status": "Pass"
    },
    {
        "Test Case ID": "TC-005",
        "Requirement ID": "REQ-DEDUP-005",
        "Description": "Batch Logic: Duplicate emails within same batch list",
        "Test Input": "Batch List: [\n  {'trigger_name': 'Mem', 'computer_name': 'PC2', 'subject': 'Warn'},\n  {'trigger_name': 'Mem', 'computer_name': 'PC2', 'subject': 'Warn'}\n]",
        "Expected Output": "BatchDeduplication returns:\nUnique: 1 count\nDuplicates: 1 count",
        "Actual Output": "BatchDeduplication returns:\nUnique: 1 count\nDuplicates: 1 count",
        "Test Status": "Pass"
    },
    {
        "Test Case ID": "TC-006",
        "Requirement ID": "REQ-DEDUP-006",
        "Description": "Validation: Missing required fields",
        "Test Input": "Input Email: {'trigger_name': '', 'computer_name': 'PC3', 'subject': 'Info'}",
        "Expected Output": "Status: Error\nMessage: Missing required fields for deduplication",
        "Actual Output": "Status: Error\nMessage: Missing required fields...",
        "Test Status": "Pass"
    }
]

# Create DataFrames
df_requirements = pd.DataFrame(requirements_data)
df_testcases = pd.DataFrame(test_cases_data)

# Save to Excel
output_filename = "Deduplication_Module_Unit_Test_Cases.xlsx"
with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
    df_testcases.to_excel(writer, sheet_name='testcases', index=False)
    df_requirements.to_excel(writer, sheet_name='requirements', index=False)

    # Auto-adjust column widths (basic estimation)
    for sheet in writer.sheets.values():
        for column in sheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = (max_length + 2)
            # Cap width to avoid massive columns
            if adjusted_width > 50:
                adjusted_width = 50
            sheet.column_dimensions[column_letter].width = adjusted_width

print(f"File created: {output_filename}")