# Imports faculty-programs data into Notion database
#
# Average Runtime: ~30 mins
# Jeff Henline - 6/4/24

import mysql.connector
import requests
import json
import configparser
import time
import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Control variable for SendGrid
ENABLE_SENDGRID = True

# Control variable for user confirmation mode
USER_CONFIRMATION_MODE = True

# Control variable for test mode
TEST_MODE = False
TEST_LIMIT = 100

# Read the configuration file
config = configparser.ConfigParser()
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
notion_database_id = '1d581093-9e74-451b-9431-ca10104b6d50'

# SendGrid API key from config.ini
sendgrid_api_key = config['auth']['sendgrid_api_key']

# Query to fetch records from MySQL
query = """
SELECT 
    faculty_program.user_id AS user_id,
    faculty_program.program_id AS program_id,
    users.email AS email,
    programs.Long_Name,
    programs.Time,
    faculty_program.DateTaken,
    GROUP_CONCAT(categories.name ORDER BY categories.name SEPARATOR ', ') AS Category
FROM 
    faculty_program
JOIN 
    users ON users.id = faculty_program.user_id
JOIN 
    programs ON programs.id = faculty_program.program_id
LEFT JOIN 
    programs_categories ON programs.id = programs_categories.program_id
LEFT JOIN 
    categories ON programs_categories.category_id = categories.id
GROUP BY 
    faculty_program.user_id, 
    faculty_program.program_id, 
    users.email, 
    programs.Long_Name, 
    programs.Time, 
    faculty_program.DateTaken;
"""

def fetch_mysql_records():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)
        
        if TEST_MODE:
            # First, get a sample of user_id/program_id pairs that exist in Notion
            logging.info(f"Test mode: Fetching {TEST_LIMIT} reference records from Notion...")
            notion_records = fetch_notion_records(fetch_only_ids=True)
            if not notion_records:
                logging.error("Failed to fetch reference records from Notion")
                return []
                
            # Create IN clause for the sample records
            id_pairs = [(record['user_id'], record['program_id']) for record in notion_records]
            id_conditions = [f"(faculty_program.user_id = {user_id} AND faculty_program.program_id = {prog_id})" 
                           for user_id, prog_id in id_pairs]
            where_clause = "WHERE " + " OR ".join(id_conditions)
            
            logging.info(f"Fetching MySQL records matching {len(id_pairs)} Notion records")
        else:
            where_clause = ""
            
        final_query = query.format(where_clause)
        cursor.execute(final_query)
        return cursor.fetchall()
    except mysql.connector.Error as err:
        logging.error(f"Error fetching MySQL records: {err}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_notion_records(fetch_only_ids=False):
    url = f'https://api.notion.com/v1/databases/{notion_database_id}/query'
    headers = {
        'Authorization': f'Bearer {notion_secret}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    }
    
    all_records = []
    has_more = True
    start_cursor = None
    total_records_fetched = 0
    retry_attempts = 0
    max_retries = 5
    unique_ids = set()

    while has_more:
        payload = {"start_cursor": start_cursor} if start_cursor else {}
        payload["page_size"] = 100
        
        if TEST_MODE:
            # Filter for program_id = 2
            payload["filter"] = {
                "and": [
                    {
                        "property": "program_id",
                        "number": {
                            "equals": 2
                        }
                    },
                    {
                        "property": "user_id",
                        "title": {
                            "is_not_empty": True
                        }
                    }
                ]
            }
        
        logging.debug(f"Fetching page with cursor: {start_cursor}")
        
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code in [429, 504]:  # Rate limit exceeded or Gateway time-out
            retry_attempts += 1
            if retry_attempts > max_retries:
                logging.error("Max retries exceeded. Exiting.")
                break
            retry_after = int(response.headers.get("Retry-After", 5)) if response.status_code == 429 else 5
            logging.warning(f"Error {response.status_code} occurred. Retry attempt {retry_attempts}/{max_retries} in {retry_after} seconds...")
            time.sleep(retry_after)
            continue

        try:
            response.raise_for_status()
            response_data = response.json()
            records = response_data.get('results', [])
            
            if fetch_only_ids:
                processed_records = [{
                    'user_id': page['properties']['user_id']['title'][0]['text']['content'],
                    'program_id': page['properties']['program_id']['number']
                } for page in records if page['properties']['user_id']['title'] 
                    and 'number' in page['properties']['program_id']]
                all_records.extend(processed_records)
            else:
                all_records.extend(records)
                
            total_records_fetched += len(records)
            
            if TEST_MODE and total_records_fetched >= TEST_LIMIT:
                logging.info(f"Test mode: Reached limit of {TEST_LIMIT} records")
                return all_records[:TEST_LIMIT]  # Return early in test mode
                
            # Add detailed logging about the records
            logging.debug(f"Page contains {len(records)} records (Total: {total_records_fetched})")
            if records:
                sample_record = records[0]
                logging.debug(f"Sample record ID: {sample_record.get('id')}")
                
            # Add more detailed pagination logging
            has_more = response_data.get('has_more', False)
            logging.debug(f"has_more: {has_more}")
            logging.debug(f"next_cursor: {response_data.get('next_cursor')}")
            
            start_cursor = response_data.get('next_cursor')
            retry_attempts = 0
            
            # Add a small delay between requests to avoid rate limiting
            time.sleep(0.3)
            
            for record in records:
                record_id = record.get('id')
                if record_id in unique_ids:
                    logging.warning(f"Duplicate record found: {record_id}")
                else:
                    unique_ids.add(record_id)
                
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
            logging.error(f"Response content: {response.content}")
            break
        except json.JSONDecodeError as json_err:
            logging.error(f"JSON decode error occurred: {json_err}")
            logging.error(f"Response content: {response.content}")
            break
        except Exception as err:
            logging.error(f"Other error occurred: {err}")
            logging.error(f"Response content: {response.content}")
            break

    if has_more and not TEST_MODE:
        # Only exit with error if we're not in test mode
        logging.error("Not all Notion records were fetched. Exiting to prevent duplicates.")
        exit(1)

    logging.info(f"Finished fetching records. Total records fetched: {total_records_fetched}")
    logging.info(f"Unique records: {len(unique_ids)}")
    return all_records

def update_notion_record(record_id, record):
    url = f'https://api.notion.com/v1/pages/{record_id}'
    headers = {
        'Authorization': f'Bearer {notion_secret}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    }

    date_taken_value = record['DateTaken']
    if isinstance(date_taken_value, datetime):
        date_taken_value = date_taken_value.date().isoformat()
    elif isinstance(date_taken_value, str):
        try:
            date_taken_value = datetime.strptime(date_taken_value, '%Y-%m-%d').date().isoformat()
        except ValueError:
            logging.error(f"Invalid date format for DateTaken: {date_taken_value}")
            return None

    properties = {
        "user_id": {
            "title": [
                {
                    "text": {
                        "content": str(record['user_id'])
                    }
                }
            ]
        },
        "program_id": {
            "number": record['program_id']
        },
        "email": {
            "rich_text": [
                {
                    "text": {
                        "content": record['email']
                    }
                }
            ]
        },
        "Long_Name": {
            "rich_text": [
                {
                    "text": {
                        "content": record['Long_Name']
                    }
                }
            ]
        },
        "Time": {
            "number": float(record['Time']) if record['Time'] and record['Time'].strip() else None
        },
        "DateTaken": {
            "date": {
                "start": date_taken_value
            }
        },
        "Category": {
            "multi_select": [  # Modified to handle multi-select
                {"name": category.strip()} for category in record['Category'].split(', ') if category.strip()
            ]
        }
    }

    data = {
        "properties": properties
    }

    try:
        response = requests.patch(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        logging.error(f"Response content: {response.content}")
    except json.JSONDecodeError as json_err:
        logging.error(f"JSON decode error occurred: {json_err}")
        logging.error(f"Response content: {response.content}")
    except Exception as err:
        logging.error(f"Other error occurred: {err}")
        logging.error(f"Response content: {response.content}")
    return None

def insert_into_notion(record):
    url = 'https://api.notion.com/v1/pages'
    headers = {
        'Authorization': f'Bearer {notion_secret}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    }

    date_taken_value = record['DateTaken']
    if isinstance(date_taken_value, datetime):
        date_taken_value = date_taken_value.date().isoformat()
    elif isinstance(date_taken_value, str):
        try:
            date_taken_value = datetime.strptime(date_taken_value, '%Y-%m-%d').date().isoformat()
        except ValueError:
            logging.error(f"Invalid date format for DateTaken: {date_taken_value}")
            return None

    properties = {
        "user_id": {
            "title": [
                {
                    "text": {
                        "content": str(record['user_id'])
                    }
                }
            ]
        },
        "program_id": {
            "number": record['program_id']
        },
        "email": {
            "rich_text": [
                {
                    "text": {
                        "content": record['email']
                    }
                }
            ]
        },
        "Long_Name": {
            "rich_text": [
                {
                    "text": {
                        "content": record['Long_Name']
                    }
                }
            ]
        },
        "Time": {
            "number": float(record['Time']) if record['Time'] and record['Time'].strip() else None
        },
        "DateTaken": {
            "date": {
                "start": date_taken_value
            }
        },
        "Category": {
            "multi_select": [  # Modified to handle multi-select
                {"name": category.strip()} for category in record['Category'].split(', ') if category.strip()
            ]
        }
    }

    data = {
        "parent": {"database_id": notion_database_id},
        "properties": properties
    }

    retry_attempts = 0
    max_retries = 5

    while retry_attempts <= max_retries:
        response = requests.post(url, headers=headers, data=json.dumps(data))

        if response.status_code == 429:  # Rate limit exceeded
            retry_attempts += 1
            retry_after = int(response.headers.get("Retry-After", 1))
            logging.warning(f"Rate limit exceeded. Retrying in {retry_after} seconds...")
            time.sleep(retry_after)
            continue

        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
            logging.error(f"Response content: {response.content}")
            break
        except json.JSONDecodeError as json_err:
            logging.error(f"JSON decode error occurred: {json_err}")
            logging.error(f"Response content: {response.content}")
            break
        except Exception as err:
            logging.error(f"Other error occurred: {err}")
            logging.error(f"Response content: {response.content}")
            break

        retry_attempts += 1

    return None

def send_summary_email(summary):
    message = Mail(
        from_email='cetltech@calstatela.edu',
        to_emails='henlij@gmail.com',
        subject='Summary of Notion Updates sent from FDMS',
        html_content=f'<p>Below is a summary of the records updated in Notion after being changed in FDMS:</p><pre>{summary}</pre>'
    )
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        logging.info(f'Email sent: {response.status_code}')
    except Exception as e:
        logging.error(f'Error sending email: {e}')

def main():
    if TEST_MODE:
        logging.info(f"Running in TEST MODE with limit of {TEST_LIMIT} records")
    
    # Record the start time
    start_time = datetime.now()
    logging.info(f"Script started at: {start_time}")

    summary = []
    mysql_records = fetch_mysql_records()
    notion_records = fetch_notion_records()
    notion_id_to_record_id = {}
    for page in notion_records:
        if ('user_id' in page['properties'] and 'title' in page['properties']['user_id'] and
                page['properties']['user_id']['title'] and
                'program_id' in page['properties'] and 'number' in page['properties']['program_id']):
            key = (page['properties']['user_id']['title'][0]['text']['content'],
                   page['properties']['program_id']['number'])
            notion_id_to_record_id[key] = page['id']

    new_records = []
    for record in mysql_records:
        key = (str(record['user_id']), record['program_id'])
        notion_record_id = notion_id_to_record_id.get(key)
        if notion_record_id:
            # Get the existing Notion record
            notion_record = next(page for page in notion_records if page['id'] == notion_record_id)
            notion_properties = notion_record['properties']

            # Create a "before" snapshot
            before_state = {
                'email': notion_properties['email']['rich_text'][0]['text']['content'] if notion_properties['email']['rich_text'] else None,
                'Long_Name': notion_properties['Long_Name']['rich_text'][0]['text']['content'] if notion_properties['Long_Name']['rich_text'] else None,
                'Time': notion_properties['Time']['number'] if 'number' in notion_properties['Time'] else None,
                'DateTaken': notion_properties['DateTaken']['date']['start'] if notion_properties['DateTaken']['date'] else None,
                'Category': {item['name'] for item in notion_properties.get('Category', {}).get('multi_select', [])}
            }

            # Create an "after" snapshot
            after_state = {
                'email': record['email'],
                'Long_Name': record['Long_Name'],
                'Time': float(record['Time']) if record['Time'] and record['Time'].strip() else None,
                'DateTaken': record['DateTaken'].strftime('%Y-%m-%d') if isinstance(record['DateTaken'], datetime) else record['DateTaken'],
                'Category': {category.strip() for category in record['Category'].split(', ') if category.strip()}
            }

            # Compare the states
            changes = {}
            for field in before_state:
                if before_state[field] != after_state[field]:
                    changes[field] = {
                        'before': before_state[field],
                        'after': after_state[field]
                    }

            if changes:
                update_response = update_notion_record(notion_record_id, record)
                logging.debug(f"Update response: {update_response}")
                
                # Create a formatted changes summary
                changes_summary = "\n".join([
                    f"    {field}:\n      Before: {details['before']}\n      After:  {details['after']}"
                    for field, details in changes.items()
                ])
                
                summary.append(
                    f'Updated record with user_id {record["user_id"]} and program_id {record["program_id"]}:\n'
                    f'Changes:\n{changes_summary}\n'
                )
            else:
                logging.debug(f"No update needed for user_id={record['user_id']}, program_id={record['program_id']}")
        else:
            new_records.append(record)

    # Modify the user confirmation section
    if USER_CONFIRMATION_MODE and new_records and not TEST_MODE:  # Added not TEST_MODE check
        logging.info(f"There are {len(new_records)} records ready to be inserted. Would you like to continue? (Yes/No)")
        user_input = input().strip().lower()
        if user_input != 'yes':
            logging.info("User opted not to insert new records.")
            new_records = []
    elif TEST_MODE and new_records:
        logging.info(f"Test mode: Skipping insertion of {len(new_records)} new records")
        new_records = []

    for record in new_records:
        logging.info(f"Inserting new record: user_id={record['user_id']}, program_id={record['program_id']}")
        insert_response = insert_into_notion(record)
        logging.debug(f"Insert response: {insert_response}")
        summary.append(
            f'Inserted record with user_id {record["user_id"]} and program_id {record["program_id"]}: {record}')
        logging.info(f'Inserted record with user_id {record["user_id"]} and program_id {record["program_id"]}')

    if ENABLE_SENDGRID and summary:
        send_summary_email('\n'.join(summary))

    # Record the end time at the end of the main function
    end_time = datetime.now()
    logging.info(f"Script ended at: {end_time}")

    # Calculate the total runtime duration
    runtime_duration = end_time - start_time

    # Optionally, format the duration to show hours, minutes, seconds
    duration_in_seconds = runtime_duration.total_seconds()
    hours, remainder = divmod(duration_in_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    logging.info(f"Script Runtime: {int(hours)}h {int(minutes)}m {seconds:.2f}s")

if __name__ == '__main__':
    main()