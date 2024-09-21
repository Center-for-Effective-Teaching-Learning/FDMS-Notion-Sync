"""
This script syncs user records between a MySQL database and a Notion database.
It performs the following tasks:

1. Fetches user records from MySQL.
2. Fetches existing records from Notion.
3. Compares and updates Notion records if changes are detected.
4. Inserts new records into Notion if they are not already present.
5. Optionally sends a summary email of the updates made.

# Average Runtime: ~5 mins
"""

import mysql.connector
import requests
import json
import configparser
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

# Updated query to fetch records from MySQL, including chair_email
query = """
SELECT DISTINCT
    users.id,
    users.first_name,
    users.last_name,
    users.email,
    users.status,
    departments.short_name AS department,
    colleges.short_name AS college,
    (
        SELECT GROUP_CONCAT(chair_users.email SEPARATOR ', ')
        FROM department_chairs dc
        INNER JOIN users chair_users ON dc.user_id = chair_users.id
        WHERE dc.department_id = users.department_id
    ) AS chair_email
FROM users
INNER JOIN departments ON departments.id = users.department_id
INNER JOIN colleges ON departments.college_id = colleges.id
LEFT JOIN faculty_program ON users.id = faculty_program.user_id
WHERE users.email LIKE '%calstatela.edu%';
"""

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
        "department": {
            "select": {
                "name": record['department']
            }
        },
        "college": {
            "select": {
                "name": record['college']
            }
        },
        "Status": {
            "select": {
                "name": record['status']
            }
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
        "department": {
            "select": {
                "name": record['department']
            }
        },
        "college": {
            "select": {
                "name": record['college']
            }
        },
        "Status": {
            "select": {
                "name": record['status']
            }
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
    for record in mysql_records:
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

            update_needed = (
                notion_properties['first_name']['rich_text'][0]['text']['content'] != record['first_name'] or
                notion_properties['last_name']['rich_text'][0]['text']['content'] != record['last_name'] or
                notion_properties['email']['title'][0]['text']['content'] != record['email'] or
                notion_department != record['department'] or
                notion_college != record['college'] or
                notion_properties['Status']['select']['name'] != record['status'] or
                notion_chair_email != record['chair_email']
            )

            if update_needed:
                summary.append(f'Updated record with id {record["id"]}: {record}')
                print(f'Updated record with id {record["id"]}')
                print(f'Summary of changes: {record}')
                update_notion_record(notion_record_id, record)
        else:
            new_records.append(record)

    # Prompt user before inserting new records
    if USER_CONFIRMATION_MODE and new_records:
        print(f"There are {len(new_records)} records ready to be inserted. Would you like to continue? (Yes/No)")
        user_input = input().strip().lower()
        if user_input != 'yes':
            print("User opted not to insert new records.")
            new_records = []

    for record in new_records:
        summary.append(f'Inserted record with id {record["id"]}: {record}')
        print(f'Inserted record with id {record["id"]}')
        print(f'Summary of changes: {record}')
        insert_into_notion(record)

    if ENABLE_SENDGRID and summary:
        send_summary_email('\n'.join(summary))

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
