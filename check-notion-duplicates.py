"""
This script checks for duplicate user records in a Notion database by email address.
It performs the following tasks:

1. Fetches all user records from Notion.
2. Groups records by email address.
3. Identifies any emails that have multiple records.
4. Provides a detailed report of duplicates found.
5. Optionally sends a summary email of the duplicates.

# Average Runtime: ~2-3 mins

Change Log
- 2025-01-27: Initial creation for duplicate detection
"""

import requests
import json
import configparser
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from datetime import datetime
from collections import defaultdict

# Control variable for SendGrid
ENABLE_SENDGRID = True

# Read the configuration file
config = configparser.ConfigParser()

# Config file for production
config.read('/home/bitnami/scripts/config.ini')

# Notion API details from config.ini
notion_secret = config['notion']['token']
notion_database_id = config['notion']['facultydb']

# SendGrid API key from config.ini
sendgrid_api_key = config['auth']['sendgrid_api_key']

def fetch_notion_records():
    """Fetch all records from Notion database"""
    url = f'https://api.notion.com/v1/databases/{notion_database_id}/query'
    headers = {
        'Authorization': f'Bearer {notion_secret}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    }
    all_records = []
    has_more = True
    start_cursor = None
    record_count = 0

    print("Fetching records from Notion...")
    while has_more:
        payload = {'page_size': 100}
        if start_cursor:
            payload['start_cursor'] = start_cursor

        response = requests.post(url, headers=headers, json=payload)
        response_data = response.json()
        fetched_records = response_data.get('results', [])
        all_records.extend(fetched_records)
        record_count += len(fetched_records)
        has_more = response_data.get('has_more', False)
        start_cursor = response_data.get('next_cursor', None)

        print(".", end="")

    print(f"\nFetched {record_count} records from Notion.")

    # Check if all records are fetched
    if has_more:
        print("Not all Notion records were fetched. Stopping the script to prevent incomplete analysis.")
        exit(1)

    return all_records

def extract_user_info(notion_record):
    """Extract user information from a Notion record"""
    try:
        properties = notion_record['properties']
        
        # Extract email (title field)
        email = ''
        if properties.get('email') and properties['email'].get('title'):
            email = properties['email']['title'][0]['text']['content']
        
        # Extract other fields
        user_id = properties.get('id', {}).get('number', 'N/A')
        first_name = ''
        if properties.get('first_name') and properties['first_name'].get('rich_text'):
            first_name = properties['first_name']['rich_text'][0]['text']['content']
        
        last_name = ''
        if properties.get('last_name') and properties['last_name'].get('rich_text'):
            last_name = properties['last_name']['rich_text'][0]['text']['content']
        
        department = ''
        if properties.get('department') and properties['department'].get('select'):
            department = properties['department']['select'].get('name', '')
        
        college = ''
        if properties.get('college') and properties['college'].get('select'):
            college = properties['college']['select'].get('name', '')
        
        status = ''
        if properties.get('Status') and properties['Status'].get('select'):
            status = properties['Status']['select'].get('name', '')
        
        chair_email = ''
        if properties.get('chair_email') and properties['chair_email'].get('rich_text'):
            chair_email = properties['chair_email']['rich_text'][0]['text']['content']
        
        return {
            'notion_id': notion_record['id'],
            'user_id': user_id,
            'email': email,
            'first_name': first_name,
            'last_name': last_name,
            'department': department,
            'college': college,
            'status': status,
            'chair_email': chair_email,
            'created_time': notion_record.get('created_time', 'N/A'),
            'last_edited_time': notion_record.get('last_edited_time', 'N/A')
        }
    except Exception as e:
        print(f"Error extracting user info from record {notion_record.get('id', 'unknown')}: {e}")
        return None

def find_duplicates(notion_records):
    """Find duplicate records by email address"""
    print("\nAnalyzing records for duplicates...")
    
    # Group records by email
    email_groups = defaultdict(list)
    invalid_records = []
    
    for record in notion_records:
        user_info = extract_user_info(record)
        if user_info:
            if user_info['email']:  # Only process records with valid emails
                email_groups[user_info['email'].lower()].append(user_info)
            else:
                invalid_records.append(user_info)
        else:
            invalid_records.append(record)
    
    # Find duplicates (emails with more than one record)
    duplicates = {email: records for email, records in email_groups.items() if len(records) > 1}
    
    # Find unique records (emails with exactly one record)
    unique_records = {email: records[0] for email, records in email_groups.items() if len(records) == 1}
    
    return duplicates, unique_records, invalid_records

def analyze_duplicates(duplicates):
    """Analyze duplicate records to understand the nature of duplicates"""
    analysis = {
        'total_duplicate_emails': len(duplicates),
        'total_duplicate_records': sum(len(records) for records in duplicates.values()),
        'duplicate_details': {}
    }
    
    for email, records in duplicates.items():
        # Group by different criteria to understand why duplicates exist
        by_name = defaultdict(list)
        by_department = defaultdict(list)
        by_status = defaultdict(list)
        
        for record in records:
            name_key = f"{record['first_name']} {record['last_name']}"
            by_name[name_key].append(record)
            by_department[record['department']].append(record)
            by_status[record['status']].append(record)
        
        analysis['duplicate_details'][email] = {
            'record_count': len(records),
            'records': records,
            'name_variations': len(by_name),
            'department_variations': len(by_department),
            'status_variations': len(by_status),
            'by_name': dict(by_name),
            'by_department': dict(by_department),
            'by_status': dict(by_status)
        }
    
    return analysis

def generate_report(duplicates, unique_records, invalid_records, analysis):
    """Generate a comprehensive report of the duplicate analysis"""
    report = []
    
    report.append("=" * 80)
    report.append("NOTION DUPLICATE USER ANALYSIS REPORT")
    report.append("=" * 80)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # Summary statistics
    report.append("SUMMARY STATISTICS")
    report.append("-" * 40)
    report.append(f"Total records analyzed: {len(duplicates) + len(unique_records) + len(invalid_records)}")
    report.append(f"Unique email addresses: {len(unique_records)}")
    report.append(f"Duplicate email addresses: {len(duplicates)}")
    report.append(f"Invalid/missing email records: {len(invalid_records)}")
    report.append(f"Total duplicate records: {analysis['total_duplicate_records']}")
    report.append("")
    
    # Duplicate details
    if duplicates:
        report.append("DUPLICATE RECORDS DETAILS")
        report.append("-" * 40)
        
        for email, records in duplicates.items():
            report.append(f"Email: {email}")
            report.append(f"  Total records: {len(records)}")
            report.append("")
            
            # Show individual records
            for i, record in enumerate(records, 1):
                report.append(f"  Record {i}:")
                report.append(f"    Notion ID: {record['notion_id']}")
                report.append(f"    User ID: {record['user_id']}")
                report.append(f"    Name: {record['first_name']} {record['last_name']}")
                report.append(f"    Department: {record['department']}")
                report.append(f"    College: {record['college']}")
                report.append(f"    Status: {record['status']}")
                report.append(f"    Chair Email: {record['chair_email']}")
                report.append(f"    Created: {record['created_time']}")
                report.append(f"    Last Edited: {record['last_edited_time']}")
                report.append("")
            
            # Analyze variations for this email
            by_name = defaultdict(list)
            by_department = defaultdict(list)
            by_status = defaultdict(list)
            
            for record in records:
                name_key = f"{record['first_name']} {record['last_name']}"
                by_name[name_key].append(record)
                by_department[record['department']].append(record)
                by_status[record['status']].append(record)
            
            # Show variations
            if len(by_name) > 1:
                report.append("    Name Variations:")
                for name, name_records in by_name.items():
                    report.append(f"      '{name}': {len(name_records)} records")
                report.append("")
            
            if len(by_department) > 1:
                report.append("    Department Variations:")
                for dept, dept_records in by_department.items():
                    report.append(f"      '{dept}': {len(dept_records)} records")
                report.append("")
            
            if len(by_status) > 1:
                report.append("    Status Variations:")
                for status, status_records in by_status.items():
                    report.append(f"      '{status}': {len(status_records)} records")
                report.append("")
            
            report.append("-" * 40)
            report.append("")
    else:
        report.append("DUPLICATE RECORDS DETAILS")
        report.append("-" * 40)
        report.append("No duplicate records found!")
        report.append("")
    
    # Invalid records
    if invalid_records:
        report.append("INVALID/MISSING EMAIL RECORDS")
        report.append("-" * 40)
        for record in invalid_records:
            if isinstance(record, dict) and 'notion_id' in record:
                report.append(f"Notion ID: {record['notion_id']}")
                report.append(f"User ID: {record.get('user_id', 'N/A')}")
                report.append(f"Name: {record.get('first_name', 'N/A')} {record.get('last_name', 'N/A')}")
                report.append(f"Email: {record.get('email', 'MISSING/INVALID')}")
                report.append("")
            else:
                report.append(f"Raw record: {record}")
                report.append("")
    
    # Recommendations
    report.append("RECOMMENDATIONS")
    report.append("-" * 40)
    if duplicates:
        report.append("1. Review all duplicate records to determine which should be kept")
        report.append("2. Consider merging duplicate records if they represent the same person")
        report.append("3. Delete duplicate records that are clearly redundant")
        report.append("4. Investigate why duplicates were created (data import issues, manual entry errors, etc.)")
        report.append("5. Implement data validation to prevent future duplicates")
    else:
        report.append("1. No duplicates found - database is clean!")
        report.append("2. Continue monitoring for duplicates during regular sync operations")
    
    report.append("")
    report.append("=" * 80)
    
    return "\n".join(report)

def send_summary_email(report):
    """Send summary email of the duplicate analysis"""
    message = Mail(
        from_email='cetltech@calstatela.edu',
        to_emails='henlij@gmail.com',
        subject='Notion Duplicate User Analysis Report',
        html_content=f'<p>Below is the duplicate analysis report for the Notion faculty database:</p><pre>{report}</pre>'
    )
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        print(f'Email sent: {response.status_code}')
    except Exception as e:
        print(f'Error sending email: {e}')

def main():
    # Record the start time
    start_time = datetime.now()
    print(f"Script started at: {start_time}")
    
    try:
        # Fetch all records from Notion
        notion_records = fetch_notion_records()
        
        if not notion_records:
            print("No records found in Notion database.")
            return
        
        # Find duplicates
        duplicates, unique_records, invalid_records = find_duplicates(notion_records)
        
        # Analyze duplicates
        analysis = analyze_duplicates(duplicates)
        
        # Generate report
        report = generate_report(duplicates, unique_records, invalid_records, analysis)
        
        # Print report to console
        print("\n" + report)
        
        # Save report to file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'notion_duplicate_report_{timestamp}.txt'
        with open(filename, 'w') as f:
            f.write(report)
        print(f"\nReport saved to: {filename}")
        
        # Send email if duplicates found and SendGrid is enabled
        if ENABLE_SENDGRID and duplicates:
            print("\nSending email report...")
            send_summary_email(report)
        
        # Final summary
        print(f"\n{'='*60}")
        print("ANALYSIS COMPLETE")
        print(f"{'='*60}")
        print(f"Total records analyzed: {len(notion_records)}")
        print(f"Unique emails: {len(unique_records)}")
        print(f"Duplicate emails: {len(duplicates)}")
        print(f"Invalid records: {len(invalid_records)}")
        
        if duplicates:
            print(f"\n⚠️  DUPLICATES FOUND: {len(duplicates)} email addresses have multiple records")
            print("   Review the detailed report above and take action to clean up duplicates.")
        else:
            print(f"\n✅ NO DUPLICATES FOUND: Database is clean!")
        
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        raise
    
    finally:
        # Record the end time
        end_time = datetime.now()
        print(f"\nScript ended at: {end_time}")
        
        # Calculate the total runtime duration
        runtime_duration = end_time - start_time
        duration_in_seconds = runtime_duration.total_seconds()
        hours, remainder = divmod(duration_in_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"Script Runtime: {int(hours)}h {int(minutes)}m {seconds:.2f}s")

if __name__ == '__main__':
    main()
