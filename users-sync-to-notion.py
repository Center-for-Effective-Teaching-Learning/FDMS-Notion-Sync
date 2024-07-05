"""
This script syncs  user records between a MySQL database and a Notion database.
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

# Config file for productions
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

# Query to fetch records from MySQL
# Query only includes users who have completed at least one program and who have
# a @calstatela.edu email address
query = """
SELECT DISTINCT users.id, first_name, last_name, email, status, departments.short_name AS department 
FROM users
INNER JOIN departments ON departments.id = users.department_id 
INNER JOIN faculty_program ON users.id = faculty_program.user_id
WHERE users.email LIKE '%calstatela.edu%'
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

    print(f"Fetched {record_count} records from Notion...")

    # Check if all records are fetched
    if has_more:
        print("Not all Notion records were fetched. Stopping the script to prevent duplicates.")
        exit(1)

    return all_records


def get_notion_record_id_by_primary_key(notion_records, record_id):
    for page in notion_records:
        if page['properties']['id']['title'][0]['text']['content'] == str(record_id):
            return page['id']
    return None


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
        "Status": {
            "select": {
                "name": record['status']
            }
        }
    }

    data = {
        "properties": properties
    }

    response = requests.patch(url, headers=headers, data=json.dumps(data))
    return response.json()


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
        "Status": {
            "select": {
                "name": record['status']
            }
        }
    }

    data = {
        "parent": {"database_id": notion_database_id},
        "properties": properties
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.json()


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
        notion_record_id = notion_id_to_record_id.get(str(record['id']))
        if notion_record_id:
            # Check for updates
            notion_record = next(page for page in notion_records if page['id'] == notion_record_id)
            notion_properties = notion_record['properties']

            update_needed = (
                notion_properties['first_name']['rich_text'][0]['text']['content'] != record['first_name'] or
                notion_properties['last_name']['rich_text'][0]['text']['content'] != record['last_name'] or
                notion_properties['email']['title'][0]['text']['content'] != record['email'] or
                notion_properties['department']['select']['name'] != record['department'] or
                notion_properties['Status']['select']['name'] != record['status']
            )

            if update_needed:
                update_response = update_notion_record(notion_record_id, record)
                summary.append(f'Updated record with id {record["id"]}: {record}')
                print(f'Updated record with id {record["id"]}')
                print(f'Summary of changes: {record}')
                #print(f'Detailed response: {update_response}')
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
        insert_response = insert_into_notion(record)
        summary.append(f'Inserted record with id {record["id"]}: {record}')
        print(f'Inserted record with id {record["id"]}')
        print(f'Summary of changes: {record}')
        #print(f'Detailed response: {insert_response}')

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
