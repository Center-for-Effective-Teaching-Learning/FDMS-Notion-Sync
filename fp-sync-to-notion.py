"""
This script syncs faculty program records between a MySQL database and a Notion database.
It performs the following tasks:

1. Fetches faculty program records from MySQL.
2. Fetches existing records from Notion.
3. Compares and updates Notion records if changes are detected.
4. Inserts new records into Notion if they are not already present.
5. Optionally sends a summary email of the updates made.

# Average Runtime: ~30 mins (Full Check Mode) / ~1-2 mins (Incremental Mode)

Change Log
- 2025-09-06: Added database column to track sync status (Henline)
"""

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

# Control variable for full check mode (set to True for initial setup or monthly validation)
FULL_CHECK_MODE = False

# Control variable for auto-disabling full check mode after initial setup
AUTO_DISABLE_FULL_CHECK = True

# Read the configuration file
config = configparser.ConfigParser()
config.read('/home/bitnami/scripts/config.ini')

# MySQL connection details from config.ini
mysql_config = {
    'host': config['mysql']['DB_HOST'],
    'user': config['mysql']['DB_USER'],
    'password': config['mysql']['DB_PASSWORD'],
    'database': config['mysql']['DB_DATABASE'],
    'autocommit': True,
    'consume_results': True
}

# Notion API details from config.ini
notion_secret = config['notion']['token']
notion_database_id = '1d581093-9e74-451b-9431-ca10104b6d50'

# SendGrid API key from config.ini
sendgrid_api_key = config['auth']['sendgrid_api_key']

# Query to fetch records from MySQL (modified to include synced_to_notion field)
query = """
SELECT 
    faculty_program.user_id AS user_id,
    faculty_program.program_id AS program_id,
    faculty_program.synced_to_notion AS synced_to_notion,
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
{where_clause}
GROUP BY 
    faculty_program.user_id, 
    faculty_program.program_id, 
    faculty_program.synced_to_notion,
    users.email, 
    programs.Long_Name, 
    programs.Time, 
    faculty_program.DateTaken
"""

def fetch_all_mysql_records():
    """Fetch all records from MySQL (for full check mode)"""
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True, buffered=True)
        final_query = query.format(where_clause="")
        cursor.execute(final_query)
        results = cursor.fetchall()
        return results
    except mysql.connector.Error as err:
        logging.error(f"Error fetching all MySQL records: {err}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_unsynced_mysql_records():
    """Fetch only records that haven't been synced to Notion (for incremental mode)"""
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True, buffered=True)
        
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
            where_clause = "WHERE faculty_program.synced_to_notion = FALSE AND (" + " OR ".join(id_conditions) + ")"
            
            logging.info(f"Fetching MySQL records matching {len(id_pairs)} Notion records")
        else:
            where_clause = "WHERE faculty_program.synced_to_notion = FALSE"
            
        # Construct the final query with WHERE before GROUP BY
        final_query = query.format(where_clause=where_clause)
        logging.debug(f"Executing query: {final_query}")
        
        cursor.execute(final_query)
        results = cursor.fetchall()
            
        return results
    except mysql.connector.Error as err:
        logging.error(f"Error fetching unsynced MySQL records: {err}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def mark_record_as_synced(user_id, program_id):
    """Mark record as successfully synced to Notion"""
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        
        update_query = """
        UPDATE faculty_program 
        SET synced_to_notion = TRUE 
        WHERE user_id = %s AND program_id = %s
        """
        
        cursor.execute(update_query, (user_id, program_id))
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Marked record as synced: user_id={user_id}, program_id={program_id}")
        return True
    except mysql.connector.Error as err:
        logging.error(f"Error marking record as synced: {err}")
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        return False

def reset_synced_flag(user_id, program_id):
    """Reset synced flag for records that need to be re-synced"""
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        
        update_query = """
        UPDATE faculty_program 
        SET synced_to_notion = FALSE 
        WHERE user_id = %s AND program_id = %s
        """
        
        cursor.execute(update_query, (user_id, program_id))
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Reset synced flag: user_id={user_id}, program_id={program_id}")
        return True
    except mysql.connector.Error as err:
        logging.error(f"Error resetting synced flag: {err}")
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        return False

def verify_notion_record_exists(notion_id, record):
    """Verify that a record actually exists in Notion"""
    url = f'https://api.notion.com/v1/pages/{notion_id}'
    headers = {
        'Authorization': f'Bearer {notion_secret}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            notion_record = response.json()
            # Verify the record matches what we expect
            notion_user_id = notion_record['properties']['user_id']['title'][0]['text']['content']
            notion_program_id = notion_record['properties']['program_id']['number']
            
            return (str(record['user_id']) == notion_user_id and 
                   record['program_id'] == notion_program_id)
        else:
            logging.warning(f"Failed to verify Notion record {notion_id}: {response.status_code}")
            return False
    except Exception as e:
        logging.error(f"Error verifying Notion record: {e}")
        return False

def validate_notion_receipt(record, notion_response):
    """Verify the record was actually created in Notion before marking as synced"""
    
    if not notion_response or 'id' not in notion_response:
        logging.error(f"Failed to create record in Notion: {record}")
        return False
    
    # Verify the record exists by fetching it back
    notion_id = notion_response['id']
    verification = verify_notion_record_exists(notion_id, record)
    
    if verification:
        # Mark as synced in MySQL
        success = mark_record_as_synced(record['user_id'], record['program_id'])
        if success:
            logging.info(f"Record verified and marked as synced: user_id={record['user_id']}, program_id={record['program_id']}")
        return success
    else:
        logging.error(f"Record verification failed for: {record}")
        return False

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
            
        final_query = query.format(where_clause=where_clause)
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
    
    # Add department and college fields if they exist in the record
    if 'department' in record and record['department'] is not None:
        properties["department"] = {
            "select": {
                "name": record['department']
            }
        }
    elif 'department' in record and record['department'] is None:
        # Handle None values by setting to empty string
        properties["department"] = {
            "select": {
                "name": ""
            }
        }
    
    if 'college' in record and record['college'] is not None:
        properties["college"] = {
            "select": {
                "name": record['college']
            }
        }
    elif 'college' in record and record['college'] is None:
        # Handle None values by setting to empty string
        properties["college"] = {
            "select": {
                "name": ""
            }
        }
    
    # Add other user fields if they exist
    if 'first_name' in record and record['first_name'] is not None:
        properties["first_name"] = {
            "rich_text": [
                {
                    "text": {
                        "content": record['first_name']
                    }
                }
            ]
        }
    
    if 'last_name' in record and record['last_name'] is not None:
        properties["last_name"] = {
            "rich_text": [
                {
                    "text": {
                        "content": record['last_name']
                    }
                }
            ]
        }
    
    if 'status' in record and record['status'] is not None:
        properties["status"] = {
            "select": {
                "name": record['status']
            }
        }
    elif 'status' in record and record['status'] is None:
        properties["status"] = {
            "select": {
                "name": ""
            }
        }
    
    if 'chair_email' in record and record['chair_email'] is not None:
        properties["chair_email"] = {
            "rich_text": [
                {
                    "text": {
                        "content": record['chair_email']
                    }
                }
            ]
        }
    elif 'chair_email' in record and record['chair_email'] is None:
        properties["chair_email"] = {
            "rich_text": [
                {
                    "text": {
                        "content": ""
                    }
                }
            ]
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

def run_full_validation():
    """Initial setup: validate all records and mark existing ones as synced"""
    
    logging.info("Starting full validation mode...")
    
    # Fetch ALL records from both sources
    all_mysql_records = fetch_all_mysql_records()
    notion_records = fetch_notion_records()
    
    logging.info(f"Fetched {len(all_mysql_records)} MySQL records and {len(notion_records)} Notion records")
    
    # Build lookup tables
    mysql_lookup = {(str(r['user_id']), r['program_id']): r for r in all_mysql_records}
    notion_lookup = {}
    
    for page in notion_records:
        if ('user_id' in page['properties'] and 'title' in page['properties']['user_id'] and
            page['properties']['user_id']['title'] and
            'program_id' in page['properties'] and 'number' in page['properties']['program_id']):
            key = (page['properties']['user_id']['title'][0]['text']['content'],
                   page['properties']['program_id']['number'])
            notion_lookup[key] = page
    
    # Mark existing records as synced
    records_marked = 0
    new_records = []
    
    for key, mysql_record in mysql_lookup.items():
        if key in notion_lookup:
            # This record already exists in Notion, mark it as synced
            if not mysql_record.get('synced_to_notion'):
                mark_record_as_synced(mysql_record['user_id'], mysql_record['program_id'])
                records_marked += 1
                logging.debug(f"Marked existing record as synced: user_id={mysql_record['user_id']}, program_id={mysql_record['program_id']}")
        else:
            # This record doesn't exist in Notion, it's new
            new_records.append(mysql_record)
            logging.debug(f"New record found: user_id={mysql_record['user_id']}, program_id={mysql_record['program_id']}")
    
    logging.info(f"Full validation complete: {records_marked} existing records marked as synced")
    logging.info(f"Found {len(new_records)} new records to sync")
    
    summary = []
    # Now process any new records that weren't in Notion
    if new_records:
        logging.info("Processing new records...")
        for record in new_records:
            logging.info(f"Inserting new record: user_id={record['user_id']}, program_id={record['program_id']}")
            insert_response = insert_into_notion(record)
            if validate_notion_receipt(record, insert_response):
                logging.info(f"Successfully synced new record: user_id={record['user_id']}, program_id={record['program_id']}")
                summary.append(f'Inserted record with user_id {record["user_id"]} and program_id {record["program_id"]}: {record}')
            else:
                logging.error(f"Failed to sync new record: user_id={record['user_id']}, program_id={record['program_id']}")
    
    # Check for orphaned records in Notion (records that don't exist in MySQL)
    orphaned_records = []
    for key, notion_record in notion_lookup.items():
        if key not in mysql_lookup:
            orphaned_records.append(notion_record)
    
    if orphaned_records:
        logging.warning(f"Found {len(orphaned_records)} orphaned records in Notion (not in MySQL)")
        for record in orphaned_records:
            logging.warning(f"Orphaned: user_id={record['properties']['user_id']['title'][0]['text']['content']}, program_id={record['properties']['program_id']['number']}")
    
    return summary

def run_incremental_sync():
    """Normal sync - only process unsynced records"""
    
    logging.info("Starting incremental sync mode...")
    
    # Only fetch records that haven't been synced
    unsynced_records = fetch_unsynced_mysql_records()
    
    if not unsynced_records:
        logging.info("No new records to sync")
        return []
    
    logging.info(f"Found {len(unsynced_records)} unsynced records")
    
    # User confirmation for new records
    if USER_CONFIRMATION_MODE and unsynced_records and not TEST_MODE:
        logging.info(f"There are {len(unsynced_records)} records ready to be inserted. Would you like to continue? (Yes/No)")
        user_input = input().strip().lower()
        if user_input != 'yes':
            logging.info("User opted not to insert new records.")
            return []
    elif TEST_MODE and unsynced_records:
        logging.info(f"Test mode: Skipping insertion of {len(unsynced_records)} new records")
        return []
    
    summary = []
    # Process each unsynced record
    for record in unsynced_records:
        logging.info(f"Inserting new record: user_id={record['user_id']}, program_id={record['program_id']}")
        insert_response = insert_into_notion(record)
        
        if validate_notion_receipt(record, insert_response):
            logging.info(f"Successfully synced: user_id={record['user_id']}, program_id={record['program_id']}")
            summary.append(f'Inserted record with user_id {record["user_id"]} and program_id {record["program_id"]}: {record}')
        else:
            logging.error(f"Failed to sync: user_id={record['user_id']}, program_id={record['program_id']}")
    
    return summary

def disable_full_check_mode():
    """Automatically disable full check mode after initial setup"""
    logging.info("=" * 60)
    logging.info("INITIAL SETUP COMPLETE!")
    logging.info("Please set FULL_CHECK_MODE = False for future runs")
    logging.info("=" * 60)

def main():
    if TEST_MODE:
        logging.info(f"Running in TEST MODE with limit of {TEST_LIMIT} records")
    
    if FULL_CHECK_MODE:
        logging.info("Running in FULL CHECK MODE - initial setup or monthly validation")
        full_summary = run_full_validation()
        if full_summary and ENABLE_SENDGRID:
            send_summary_email('\n'.join(full_summary))
        
        if AUTO_DISABLE_FULL_CHECK:
            # Automatically disable full check mode after first run
            disable_full_check_mode()
            logging.info("Full check mode disabled. Future runs will use incremental sync.")
    else:
        logging.info("Running in INCREMENTAL SYNC MODE - only processing new records")
        incremental_summary = run_incremental_sync()
        if incremental_summary and ENABLE_SENDGRID:
            send_summary_email('\n'.join(incremental_summary))
    
    # Record the start time
    start_time = datetime.now()
    logging.info(f"Script started at: {start_time}")

    summary = []
    
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