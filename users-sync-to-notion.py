"""
This script syncs user records between a MySQL database and a Notion database.
It performs the following tasks:

1. Fetches user records from MySQL.
2. Fetches existing records from Notion.
3. Compares and updates Notion records if changes are detected.
4. Inserts new records into Notion if they are not already present.
5. Optionally sends a summary email of the updates made.

# Average Runtime: ~5 mins

Change Log
- 2025-07-02: Added user confirmation mode and updated the query to fetch records from new database structure.
- 2025-08-15: Fixed duplicate records issue by ensuring exactly one assignment per user when multiple assignments exist in the same term (Henline)
"""

import mysql.connector
import requests
import json
import configparser
import time
import random
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from datetime import datetime

# Control variable for SendGrid
ENABLE_SENDGRID = True

# Control variable for user confirmation mode
USER_CONFIRMATION_MODE = True

# Read the configuration file
config = configparser.ConfigParser()

# Config file for test environment
# config.read('config.ini')

# Config file for production
config.read('/home/bitnami/scripts/config.ini')

# MySQL connection details from config.ini
mysql_config = {
    'host': config['mysql']['DB_HOST'],
    'user': config['mysql']['DB_USER'],
    'password': config['mysql']['DB_PASSWORD'],
    'database': config['mysql']['DB_DATABASE']
}

# Notion API details from config.ini
notion_secret = config['notion']['token']
notion_database_id = config['notion']['facultydb']

# SendGrid API key from config.ini
sendgrid_api_key = config['auth']['sendgrid_api_key']

# Updated query to fetch records from MySQL, including chair_email and job status
# This ensures exactly one assignment per user by selecting the assignment with the lowest ID
# when multiple assignments exist in the same term
query = """
SELECT DISTINCT
    users.id,
    users.first_name,
    users.last_name,
    users.email,
    job.job_name_pretty AS status,
    departments.short_name AS department,
    colleges.short_name AS college,
    (
        SELECT GROUP_CONCAT(chair_users.email SEPARATOR ', ')
        FROM department_chairs dc
        INNER JOIN users chair_users ON dc.user_id = chair_users.id
        WHERE dc.department_id = ua_current.department_id
    ) AS chair_email
FROM users
LEFT JOIN (
    SELECT ua.user_id, ua.job_id, ua.department_id, t.Term as term_name
    FROM user_assignments ua
    INNER JOIN terms t ON ua.term_id = t.term_id
    INNER JOIN (
        SELECT ua2.user_id, MAX(t2.PS_code) as max_ps_code
        FROM user_assignments ua2
                        INNER JOIN terms t2 ON ua2.term_id = t2.term_id
                        GROUP BY ua2.user_id
    ) latest_terms ON ua.user_id = latest_terms.user_id AND t.PS_code = latest_terms.max_ps_code
    WHERE ua.id = (
        SELECT MIN(ua3.id) 
        FROM user_assignments ua3 
        INNER JOIN terms t3 ON ua3.term_id = t3.term_id
        WHERE ua3.user_id = ua.user_id AND t3.PS_code = latest_terms.max_ps_code
    )
) ua_current ON users.id = ua_current.user_id
LEFT JOIN job ON CAST(ua_current.job_id AS CHAR) = job.job_code
LEFT JOIN departments ON ua_current.department_id = departments.id
LEFT JOIN colleges ON departments.college_id = colleges.id
LEFT JOIN faculty_program ON users.id = faculty_program.user_id
WHERE users.email LIKE '%calstatela.edu%';
"""

def normalize_value(value):
    """Normalize values for comparison, handling None vs empty string cases"""
    if value is None:
        return ''
    return str(value).strip()

def values_are_equal(val1, val2):
    """Compare two values, treating None and empty string as equal"""
    return normalize_value(val1) == normalize_value(val2)

def fetch_mysql_records():
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records

def fetch_notion_records():
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

    print(f"Fetched {record_count} records from Notion.")

    # Check if all records are fetched
    if has_more:
        print("Not all Notion records were fetched. Stopping the script to prevent duplicates.")
        exit(1)

    return all_records

def update_notion_record(record_id, record):
    url = f'https://api.notion.com/v1/pages/{record_id}'
    headers = {
        'Authorization': f'Bearer {notion_secret}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    }

    properties = {
        "email": {
            "title": [
                {
                    "text": {
                        "content": str(record['email'])
                    }
                }
            ]
        },
        "first_name": {
            "rich_text": [
                {
                    "text": {
                        "content": record['first_name']
                    }
                }
            ]
        },
        "last_name": {
            "rich_text": [
                {
                    "text": {
                        "content": record['last_name']
                    }
                }
            ]
        },
        "id": {
            "number": record['id']
        },
        "chair_email": {
            "rich_text": [
                {
                    "text": {
                        "content": record['chair_email'] if record['chair_email'] else ''
                    }
                }
            ]
        }
    }
    
    # Only add department select if department is not None
    if record['department'] is not None:
        properties["department"] = {
            "select": {
                "name": record['department']
            }
        }
    
    # Only add college select if college is not None
    if record['college'] is not None:
        properties["college"] = {
            "select": {
                "name": record['college']
            }
        }
    
    # Only add Status select if status is not None
    if record['status'] is not None:
        properties["Status"] = {
            "select": {
                "name": record['status']
            }
        }

    data = {
        "properties": properties
    }

    response = requests.patch(url, headers=headers, data=json.dumps(data))

    # Add error handling
    if not response.ok:
        print(f"\nFailed to update record {record_id}, status code: {response.status_code}")
        print(f"Response content: {response.text}")
        # Optionally, you can raise an exception here to stop the script
        # response.raise_for_status()
    else:
        try:
            return response.json()
        except json.JSONDecodeError as e:
            print(f"\nError decoding JSON response for record {record_id}: {e}")
            print(f"Response content: {response.text}")
            raise

def insert_into_notion(record):
    url = 'https://api.notion.com/v1/pages'
    headers = {
        'Authorization': f'Bearer {notion_secret}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    }

    properties = {
        "email": {
            "title": [
                {
                    "text": {
                        "content": str(record['email'])
                    }
                }
            ]
        },
        "first_name": {
            "rich_text": [
                {
                    "text": {
                        "content": record['first_name']
                    }
                }
            ]
        },
        "last_name": {
            "rich_text": [
                {
                    "text": {
                        "content": record['last_name']
                    }
                }
            ]
        },
        "id": {
            "number": record['id']
        },
        "chair_email": {
            "rich_text": [
                {
                    "text": {
                        "content": record['chair_email'] if record['chair_email'] else ''
                    }
                }
            ]
        }
    }
    
    # Only add department select if department is not None
    if record['department'] is not None:
        properties["department"] = {
            "select": {
                "name": record['department']
            }
        }
    
    # Only add college select if college is not None
    if record['college'] is not None:
        properties["college"] = {
            "select": {
                "name": record['college']
            }
        }
    
    # Only add Status select if status is not None
    if record['status'] is not None:
        properties["Status"] = {
            "select": {
                "name": record['status']
            }
        }

    data = {
        "parent": {"database_id": notion_database_id},
        "properties": properties
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    # Add error handling
    if not response.ok:
        print(f"\nFailed to insert record {record['id']}, status code: {response.status_code}")
        print(f"Response content: {response.text}")
        # Optionally, you can raise an exception here
        # response.raise_for_status()
    else:
        try:
            return response.json()
        except json.JSONDecodeError as e:
            print(f"\nError decoding JSON response for record {record['id']}: {e}")
            print(f"Response content: {response.text}")
            raise

def refresh_notion_records():
    """Refresh Notion records to get the latest data after updates"""
    print("\nRefreshing Notion records to verify updates...")
    return fetch_notion_records()

def send_summary_email(summary):
    message = Mail(
        from_email='cetltech@calstatela.edu',
        to_emails='henlij@gmail.com',
        subject='Summary of Notion User Updates sent from FDMS',
        html_content=f'<p>Below is a summary of the records inserted/updated in Notion after being added/changed '
                     f'in FDMS:</p><pre>{summary}</pre>'
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

    summary = []
    mysql_records = fetch_mysql_records()
    notion_records = fetch_notion_records()

    # Create a dictionary to map notion record ids to record ids from MySQL
    notion_id_to_record_id = {}
    for page in notion_records:
        try:
            record_id = page['properties']['id']['number']
            notion_id_to_record_id[str(record_id)] = page['id']
        except KeyError as e:
            print(f"Error processing page: {e}, page content: {json.dumps(page, indent=2)}")

    new_records = []
    update_records = []
    
    # Debug: Track comparison details for random records
    debug_count = 0
    max_debug = 5
    debug_indices = random.sample(range(len(mysql_records)), min(max_debug, len(mysql_records)))
    print(f"\nDebug: Will show details for {len(debug_indices)} random records (indices: {sorted(debug_indices)})")
    
    for i, record in enumerate(mysql_records):
        # Handle cases where 'chair_email' might be None
        if 'chair_email' not in record or record['chair_email'] is None:
            record['chair_email'] = ''

        notion_record_id = notion_id_to_record_id.get(str(record['id']))
        if notion_record_id:
            # Check for updates
            notion_record = next(page for page in notion_records if page['id'] == notion_record_id)
            notion_properties = notion_record['properties']

            # Handle cases where 'college' or 'department' might be None
            notion_college = ''
            notion_department = ''
            if notion_properties.get('college') and notion_properties['college'].get('select'):
                notion_college = notion_properties['college']['select'].get('name', '')
            if notion_properties.get('department') and notion_properties['department'].get('select'):
                notion_department = notion_properties['department']['select'].get('name', '')

            # Handle cases where 'chair_email' might be missing or empty
            notion_chair_email = ''
            if notion_properties.get('chair_email') and notion_properties['chair_email'].get('rich_text'):
                notion_chair_email = notion_properties['chair_email']['rich_text'][0]['text']['content']

            # Handle cases where 'status' might be None (due to LEFT JOIN)
            mysql_status = record['status'] if record['status'] else ''
            notion_status = ''
            if notion_properties.get('Status') and notion_properties['Status'].get('select'):
                notion_status = notion_properties['Status']['select'].get('name', '')

            # Debug logging for random records
            if i in debug_indices:
                print(f"\nDEBUG Record {record['id']} ({record['first_name']} {record['last_name']}):")
                print(f"  MySQL: first_name='{record['first_name']}', last_name='{record['last_name']}', email='{record['email']}'")
                print(f"  MySQL: department='{record['department']}', college='{record['college']}', status='{mysql_status}', chair_email='{record['chair_email']}'")
                print(f"  Notion: first_name='{notion_properties['first_name']['rich_text'][0]['text']['content']}', last_name='{notion_properties['last_name']['rich_text'][0]['text']['content']}', email='{notion_properties['email']['title'][0]['text']['content']}'")
                print(f"  Notion: department='{notion_department}', college='{notion_college}', status='{notion_status}', chair_email='{notion_chair_email}'")

            update_needed = (
                not values_are_equal(notion_properties['first_name']['rich_text'][0]['text']['content'], record['first_name']) or
                not values_are_equal(notion_properties['last_name']['rich_text'][0]['text']['content'], record['last_name']) or
                not values_are_equal(notion_properties['email']['title'][0]['text']['content'], record['email']) or
                not values_are_equal(notion_department, record['department']) or
                not values_are_equal(notion_college, record['college']) or
                not values_are_equal(notion_status, mysql_status) or
                not values_are_equal(notion_chair_email, record['chair_email'])
            )

            if update_needed:
                update_records.append((notion_record_id, record))
                # Debug: Show why update is needed for random records
                if i in debug_indices:
                    print(f"  UPDATE NEEDED because:")
                    if notion_properties['first_name']['rich_text'][0]['text']['content'] != record['first_name']:
                        print(f"    first_name: '{notion_properties['first_name']['rich_text'][0]['text']['content']}' != '{record['first_name']}'")
                    if notion_properties['last_name']['rich_text'][0]['text']['content'] != record['last_name']:
                        print(f"    last_name: '{notion_properties['last_name']['rich_text'][0]['text']['content']}' != '{record['last_name']}'")
                    if notion_properties['email']['title'][0]['text']['content'] != record['email']:
                        print(f"    email: '{notion_properties['email']['title'][0]['text']['content']}' != '{record['email']}'")
                    if notion_department != record['department']:
                        print(f"    department: '{notion_department}' != '{record['department']}'")
                    if notion_college != record['college']:
                        print(f"    college: '{notion_college}' != '{record['college']}'")
                    if notion_status != mysql_status:
                        print(f"    status: '{notion_status}' != '{mysql_status}'")
                    if notion_chair_email != record['chair_email']:
                        print(f"    chair_email: '{notion_chair_email}' != '{record['chair_email']}'")
        else:
            new_records.append(record)

    # Show summary counts before confirmation prompts
    print(f"\n{'='*60}")
    print("SYNC SUMMARY")
    print(f"{'='*60}")
    print(f"Total records in FDMS: {len(mysql_records)}")
    print(f"Total records in Notion: {len(notion_records)}")
    print(f"Records to be updated: {len(update_records)}")
    print(f"Records to be inserted: {len(new_records)}")
    print(f"Records that are already in sync: {len(mysql_records) - len(update_records) - len(new_records)}")
    
    # Find records that exist in Notion but not in FDMS
    mysql_ids = {str(record['id']) for record in mysql_records}
    orphaned_notion_records = []
    
    for notion_record in notion_records:
        try:
            notion_id = str(notion_record['properties']['id']['number'])
            if notion_id not in mysql_ids:
                # Extract basic info from the orphaned record
                properties = notion_record['properties']
                first_name = properties.get('first_name', {}).get('rich_text', [{}])[0].get('text', {}).get('content', 'N/A')
                last_name = properties.get('last_name', {}).get('rich_text', [{}])[0].get('text', {}).get('content', 'N/A')
                email = properties.get('email', {}).get('title', [{}])[0].get('text', {}).get('content', 'N/A')
                
                # Handle department and college fields that might be None
                department = ''
                if properties.get('department') and properties['department'].get('select'):
                    department = properties['department']['select'].get('name', '')
                
                college = ''
                if properties.get('college') and properties['college'].get('select'):
                    college = properties['college']['select'].get('name', '')
                
                status = ''
                if properties.get('Status') and properties['Status'].get('select'):
                    status = properties['Status']['select'].get('name', '')
                
                orphaned_notion_records.append({
                    'notion_id': notion_record['id'],
                    'mysql_id': notion_id,
                    'first_name': first_name,
                    'last_name': last_name,
                    'email': email,
                    'department': department,
                    'college': college,
                    'status': status
                })
        except (KeyError, TypeError) as e:
            print(f"Error processing orphaned Notion record: {e}")
            continue
    
    if orphaned_notion_records:
        print(f"\nRecords in Notion but NOT in FDMS: {len(orphaned_notion_records)}")
        print("-" * 80)
        for i, record in enumerate(orphaned_notion_records[:20]):  # Show first 20
            print(f"{i+1:2d}. Notion ID: {record['notion_id'][:8]}... | MySQL ID: {record['mysql_id']} | {record['first_name']} {record['last_name']} | {record['email']}")
            print(f"    Dept: {record['department']} | College: {record['college']} | Status: {record['status']}")
        if len(orphaned_notion_records) > 20:
            print(f"... and {len(orphaned_notion_records) - 20} more orphaned records")
        print("-" * 80)
    else:
        print(f"\n✓ No orphaned records found - all Notion records have corresponding FDMS records")
    
    # Show sample of what's being updated
    if update_records:
        print(f"\nSample of fields being updated:")
        sample_records = update_records[:3]
        for notion_record_id, record in sample_records:
            notion_record = next(page for page in notion_records if page['id'] == notion_record_id)
            notion_properties = notion_record['properties']
            
            # Extract current Notion values
            notion_first_name = notion_properties['first_name']['rich_text'][0]['text']['content']
            notion_last_name = notion_properties['last_name']['rich_text'][0]['text']['content']
            notion_email = notion_properties['email']['title'][0]['text']['content']
            # Handle department field safely
            notion_department = ''
            if notion_properties.get('department') and notion_properties['department'].get('select'):
                notion_department = notion_properties['department']['select'].get('name', '')
            
            # Handle college field safely
            notion_college = ''
            if notion_properties.get('college') and notion_properties['college'].get('select'):
                notion_college = notion_properties['college']['select'].get('name', '')
            
            # Handle status field safely
            notion_status = ''
            if notion_properties.get('Status') and notion_properties['Status'].get('select'):
                notion_status = notion_properties['Status']['select'].get('name', '')
            notion_chair_email = ''
            if notion_properties.get('chair_email') and notion_properties['chair_email'].get('rich_text'):
                notion_chair_email = notion_properties['chair_email']['rich_text'][0]['text']['content']
            
            print(f"  ID {record['id']}:")
            if notion_first_name != record['first_name']:
                print(f"    first_name: '{notion_first_name}' → '{record['first_name']}'")
            if notion_last_name != record['last_name']:
                print(f"    last_name: '{notion_last_name}' → '{record['last_name']}'")
            if notion_email != record['email']:
                print(f"    email: '{notion_email}' → '{record['email']}'")
            if notion_department != record['department']:
                print(f"    department: '{notion_department}' → '{record['department']}'")
            if notion_college != record['college']:
                print(f"    college: '{notion_college}' → '{record['college']}'")
            if notion_status != record['status']:
                print(f"    status: '{notion_status}' → '{record['status']}'")
            if notion_chair_email != record['chair_email']:
                print(f"    chair_email: '{notion_chair_email}' → '{record['chair_email']}'")
    
    print(f"{'='*60}\n")

    # Prompt user before updating existing records
    if USER_CONFIRMATION_MODE and update_records:
        print(f"There are {len(update_records)} records ready to be updated.")
        print("\nFirst 100 records to be updated (Current Notion → New MySQL):")
        print("Note: 'Current Notion' values are from initial fetch and may not reflect recent updates")
        print("-" * 120)
        for i, (notion_record_id, record) in enumerate(update_records[:100]):
            # Get current Notion values for comparison
            notion_record = next(page for page in notion_records if page['id'] == notion_record_id)
            notion_properties = notion_record['properties']
            
            # Extract current Notion values
            notion_first_name = notion_properties['first_name']['rich_text'][0]['text']['content']
            notion_last_name = notion_properties['last_name']['rich_text'][0]['text']['content']
            notion_email = notion_properties['email']['title'][0]['text']['content']
            # Handle department field safely
            notion_department = ''
            if notion_properties.get('department') and notion_properties['department'].get('select'):
                notion_department = notion_properties['department']['select'].get('name', '')
            
            # Handle status field safely
            notion_status = ''
            if notion_properties.get('Status') and notion_properties['Status'].get('select'):
                notion_status = notion_properties['Status']['select'].get('name', '')
            
            print(f"{i+1:2d}. ID: {record['id']:4d}")
            print(f"    Name: {notion_first_name} {notion_last_name} → {record['first_name']} {record['last_name']}")
            print(f"    Email: {notion_email} → {record['email']}")
            print(f"    Dept:  {notion_department} → {record['department']}")
            print(f"    Status: {notion_status} → {record['status']}")
            print()
        if len(update_records) > 100:
            print(f"... and {len(update_records) - 100} more records")
        print("-" * 120)
        print("Would you like to continue? (Yes/No)")
        user_input = input().strip().lower()
        if user_input != 'yes':
            print("User opted not to update records.")
            update_records = []

    for notion_record_id, record in update_records:
        summary.append(f'Updated record with id {record["id"]}: {record}')
        print(f'Updating record with id {record["id"]}')
        print(f'Summary of changes: {record}')
        
        try:
            result = update_notion_record(notion_record_id, record)
            if result:
                print(f'✓ Successfully updated record {record["id"]}')
            else:
                print(f'⚠ Update may have failed for record {record["id"]}')
        except Exception as e:
            print(f'✗ Error updating record {record["id"]}: {e}')
            summary.append(f'ERROR updating record {record["id"]}: {e}')
        
        # Small delay to avoid rate limiting
        time.sleep(0.1)

    # Refresh Notion records to get the latest data after updates
    notion_records = refresh_notion_records()
    
    # Verify updates were successful by re-checking a few updated records
    if update_records:
        print(f"\nVerifying updates for first 3 updated records:")
        verification_count = 0
        for notion_record_id, record in update_records[:3]:
            notion_record = next(page for page in notion_records if page['id'] == notion_record_id)
            notion_properties = notion_record['properties']
            
            # Extract current Notion values after update
            notion_first_name = notion_properties['first_name']['rich_text'][0]['text']['content']
            notion_last_name = notion_properties['last_name']['rich_text'][0]['text']['content']
            notion_email = notion_properties['email']['title'][0]['text']['content']
            
            # Handle department field - could be None if not set
            notion_department = ''
            if notion_properties.get('department') and notion_properties['department'].get('select'):
                notion_department = notion_properties['department']['select'].get('name', '')
            
            # Handle college field - could be None if not set
            notion_college = ''
            if notion_properties.get('college') and notion_properties['college'].get('select'):
                notion_college = notion_properties['college']['select'].get('name', '')
            
            # Handle status field - could be None if not set
            notion_status = ''
            if notion_properties.get('Status') and notion_properties['Status'].get('select'):
                notion_status = notion_properties['Status']['select'].get('name', '')
            
            notion_chair_email = ''
            if notion_properties.get('chair_email') and notion_properties['chair_email'].get('rich_text'):
                notion_chair_email = notion_properties['chair_email']['rich_text'][0]['text']['content']
            
            print(f"  Record {record['id']} ({record['first_name']} {record['last_name']}):")
            print(f"    MySQL:  {record['first_name']} | {record['last_name']} | {record['email']} | {record['department']} | {record['college']} | {record['status']} | {record['chair_email']}")
            print(f"    Notion: {notion_first_name} | {notion_last_name} | {notion_email} | {notion_department} | {notion_college} | {notion_status} | {notion_chair_email}")
            print(f"    Match:  {values_are_equal(notion_first_name, record['first_name'])} | {values_are_equal(notion_last_name, record['last_name'])} | {values_are_equal(notion_email, record['email'])} | {values_are_equal(notion_department, record['department'])} | {values_are_equal(notion_college, record['college'])} | {values_are_equal(notion_status, record['status'])} | {values_are_equal(notion_chair_email, record['chair_email'])}")
            print()

    # Prompt user before inserting new records
    if USER_CONFIRMATION_MODE and new_records:
        print(f"There are {len(new_records)} records ready to be inserted.")
        print("\nFirst 100 records to be inserted (New MySQL values):")
        print("-" * 80)
        for i, record in enumerate(new_records[:100]):
            print(f"{i+1:2d}. ID: {record['id']:4d} | {record['first_name']} {record['last_name']} | {record['email']} | {record['department']} | {record['status']}")
        if len(new_records) > 100:
            print(f"... and {len(new_records) - 100} more records")
        print("-" * 80)
        print("Would you like to continue? (Yes/No)")
        user_input = input().strip().lower()
        if user_input != 'yes':
            print("User opted not to insert new records.")
            new_records = []

    for record in new_records:
        summary.append(f'Inserted record with id {record["id"]}: {record}')
        print(f'Inserting record with id {record["id"]}')
        print(f'Summary of changes: {record}')
        
        try:
            result = insert_into_notion(record)
            if result:
                print(f'✓ Successfully inserted record {record["id"]}')
            else:
                print(f'⚠ Insert may have failed for record {record["id"]}')
        except Exception as e:
            print(f'✗ Error inserting record {record["id"]}: {e}')
            summary.append(f'ERROR inserting record {record["id"]}: {e}')
        
        # Small delay to avoid rate limiting
        time.sleep(0.1)

    if ENABLE_SENDGRID and summary:
        send_summary_email('\n'.join(summary))

    # Final verification: check if there are still any differences
    print(f"\n{'='*60}")
    print("FINAL VERIFICATION")
    print(f"{'='*60}")
    
    # Refresh one more time to get the final state
    final_notion_records = fetch_notion_records()
    
    # Re-check for any remaining differences
    remaining_updates = 0
    for record in mysql_records:
        notion_record_id = notion_id_to_record_id.get(str(record['id']))
        if notion_record_id:
            notion_record = next((page for page in final_notion_records if page['id'] == notion_record_id), None)
            if notion_record:
                notion_properties = notion_record['properties']
                
                # Extract current Notion values
                notion_first_name = notion_properties['first_name']['rich_text'][0]['text']['content']
                notion_last_name = notion_properties['last_name']['rich_text'][0]['text']['content']
                notion_email = notion_properties['email']['title'][0]['text']['content']
                
                # Handle department field - could be None if not set
                notion_department = ''
                if notion_properties.get('department') and notion_properties['department'].get('select'):
                    notion_department = notion_properties['department']['select'].get('name', '')
                
                # Handle college field - could be None if not set
                notion_college = ''
                if notion_properties.get('college') and notion_properties['college'].get('select'):
                    notion_college = notion_properties['college']['select'].get('name', '')
                
                # Handle status field - could be None if not set
                notion_status = ''
                if notion_properties.get('Status') and notion_properties['Status'].get('select'):
                    notion_status = notion_properties['Status']['select'].get('name', '')
                
                notion_chair_email = ''
                if notion_properties.get('chair_email') and notion_properties['chair_email'].get('rich_text'):
                    notion_chair_email = notion_properties['chair_email']['rich_text'][0]['text']['content']
                
                # Check if still needs update
                still_needs_update = (
                    not values_are_equal(notion_first_name, record['first_name']) or
                    not values_are_equal(notion_last_name, record['last_name']) or
                    not values_are_equal(notion_email, record['email']) or
                    not values_are_equal(notion_department, record['department']) or
                    not values_are_equal(notion_college, record['college']) or
                    not values_are_equal(notion_status, record['status'] if record['status'] else '') or
                    not values_are_equal(notion_chair_email, record['chair_email'] if record['chair_email'] else '')
                )
                
                if still_needs_update:
                    remaining_updates += 1
                    if remaining_updates <= 3:  # Show first 3 remaining issues
                        print(f"  ID {record['id']} still needs update:")
                        if not values_are_equal(notion_first_name, record['first_name']):
                            print(f"    first_name: '{notion_first_name}' != '{record['first_name']}'")
                        if not values_are_equal(notion_last_name, record['last_name']):
                            print(f"    last_name: '{notion_last_name}' != '{record['last_name']}'")
                        if not values_are_equal(notion_email, record['email']):
                            print(f"    email: '{notion_email}' != '{record['email']}'")
                        if not values_are_equal(notion_department, record['department']):
                            print(f"    department: '{notion_department}' != '{record['department']}'")
                        if not values_are_equal(notion_college, record['college']):
                            print(f"    college: '{notion_college}' != '{record['college']}'")
                        if not values_are_equal(notion_status, record['status'] if record['status'] else ''):
                            print(f"    status: '{notion_status}' != '{record['status']}'")
                        if not values_are_equal(notion_chair_email, record['chair_email'] if record['chair_email'] else ''):
                            print(f"    chair_email: '{notion_chair_email}' != '{record['chair_email']}'")
    
    if remaining_updates == 0:
        print("✓ All records are now in sync!")
    else:
        print(f"⚠ {remaining_updates} records still need updates (showing first 3 above)")
    
    print(f"{'='*60}")

    # Record the end time at the end of the main function
    end_time = datetime.now()
    print(f"Script ended at: {end_time}")

    # Calculate the total runtime duration
    runtime_duration = end_time - start_time

    # Optionally, format the duration to show hours, minutes, seconds
    duration_in_seconds = runtime_duration.total_seconds()
    hours, remainder = divmod(duration_in_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f"Script Runtime: {int(hours)}h {int(minutes)}m {seconds:.2f}s")

if __name__ == '__main__':
    main()
