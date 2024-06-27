"""
This script maintains relationships between faculty and programs in a Notion database.

Main Tasks:
1. **Load Configuration**: Reads API token and database IDs from `config.ini`.
2. **Query Databases**: Retrieves records from faculty and programs databases with pagination and retries.
3. **Map Data**: Maps faculty emails to their Notion page IDs.
4. **Manage Relations**: Loads existing relations to avoid duplicates.
5. **Update Relations**: Matches program emails to faculty, updates Notion, and saves new relations.

Run Times:
Initial (empty existing_relations_new.txt file): ~20 hours
Subsequent: ~30 mins
"""

import requests
import time
import os
import configparser
from datetime import datetime

RELATIONS_FILE = "existing_relations_new.txt"
MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds


def query_database(database_id, token, start_cursor=None):
    """
    Queries a Notion database with pagination support and retries on failure.

    Args:
        database_id (str): The ID of the Notion database to query.
        token (str): The API token for authentication.
        start_cursor (str, optional): The starting cursor for pagination. Defaults to None.

    Returns:
        dict: The response data from the Notion API.
    """
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    data = {}
    if start_cursor:
        data["start_cursor"] = start_cursor

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 429:  # Rate limiting
                print(f"Rate limit exceeded. Waiting for {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"Request exception: {req_err}")
        except ValueError as json_err:
            print(f"JSON decode error: {json_err}")

        print(f"Retrying ({attempt + 1}/{MAX_RETRIES})...")
        time.sleep(RETRY_DELAY)

    raise Exception("Failed to query the database after several attempts")

def update_relation(page_id, related_page_ids, token, relation_property_id):
    """
    Updates the relation between a program and faculty members in Notion.

    Args:
        page_id (str): The ID of the program page to update the relation for.
        related_page_ids (list): The list of faculty page IDs to relate to the program.
        token (str): The API token for authentication.
        relation_property_id (str): The property ID for the relation.

    Returns:
        dict: The response data from the Notion API if the update is successful, else None.
    """
    url = f"https://api.notion.com/v1/pages/{page_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    data = {
        "properties": {
            relation_property_id: {
                "relation": [{"id": id} for id in related_page_ids]
            }
        }
    }
    response = requests.patch(url, headers=headers, json=data)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error updating relation for page {page_id}")
        print(f"Status code: {response.status_code}")
        #print(f"Response content: {response.content}")
        return None

def load_existing_relations():
    """
    Loads the existing relations from the local file.

    Returns:
        set: A set containing the existing relations.
    """
    if os.path.exists(RELATIONS_FILE):
        with open(RELATIONS_FILE, "r") as file:
            return set(line.strip() for line in file)
    return set()

def save_relation(relation):
    """
    Saves a single relation to the local file.

    Args:
        relation (str): The relation to save in the format "program_id,faculty_id".
    """
    with open(RELATIONS_FILE, "a") as file:
        file.write(relation + "\n")

def main():
    """
    The main function that orchestrates the matching process.
    """
    start_time = datetime.now()
    print(f"Script started at: {start_time}")

    # Load configuration
    config = configparser.ConfigParser()
    config.read('/home/bitnami/scripts/config.ini')
    token = config.get('notion', 'token')
    faculty_db_id = config.get('notion', 'facultydb')
    programs_db_id = config.get('notion', 'facultyprogramdb')

    # The property ID for the relation
    relation_property_id = 'YFSh'  # Adjust this to the correct property ID for the relation

    # Query databases to retrieve records with pagination
    faculty_records = []
    programs_records = []

    # Fetch faculty records
    start_cursor = None
    faculty_count = 0
    print("Fetching faculty records...")
    while True:
        faculty_data = query_database(faculty_db_id, token, start_cursor)
        # print(f"API Response for faculty query: {faculty_data}")  # Log the API response for debugging
        if 'results' not in faculty_data:
            print(f"Unexpected response structure: {faculty_data}")  # Log unexpected response structure
            break
        faculty_records.extend(faculty_data['results'])
        faculty_count += len(faculty_data['results'])
        print(".", end="", flush=True)
        if not faculty_data.get('has_more'):
            break
        start_cursor = faculty_data.get('next_cursor')
    print("Faculty records fetched.")

    # Fetch program records
    start_cursor = None
    program_count = 0
    print("Fetching program records...")
    while True:
        programs_data = query_database(programs_db_id, token, start_cursor)
        # print(f"API Response for programs query: {programs_data}")  # Log the API response for debugging
        if 'results' not in programs_data:
            print(f"Unexpected response structure: {programs_data}")  # Log unexpected response structure
            break
        programs_records.extend(programs_data['results'])
        program_count += len(programs_data['results'])
        print(".", end="", flush=True)
        if not programs_data.get('has_more'):
            break
        start_cursor = programs_data.get('next_cursor')
    print("Program records fetched.")

    # Dictionary to map faculty email to their page ID
    faculty_email_to_id = {}
    for rec in faculty_records:
        email_data = rec['properties'].get('email', {}).get('title')
        if email_data:
            email = email_data[0]['plain_text'].strip().lower()
            faculty_email_to_id[email] = rec['id']
            # print(f"Faculty email: {email}, ID: {rec['id']}")
        else:
            print(f"Faculty record {rec['id']} does not have a valid email")

    # Load existing relations from the file
    existing_relations = load_existing_relations()

    # Initialize a counter for new matches
    new_match_count = 0

    # Match programs to faculty based on the email and update relations
    for program in programs_records:
        prog_email_data = program['properties'].get('email', {}).get('rich_text')
        if prog_email_data:
            prog_email = prog_email_data[0]['plain_text'].strip().lower()
            if prog_email in faculty_email_to_id:
                faculty_id = faculty_email_to_id[prog_email]
                relation_key = f"{program['id']},{faculty_id}"

                if relation_key not in existing_relations:
                    print(f"Matching Program {program['id']} to Faculty {faculty_id}")
                    update_response = update_relation(program['id'], [faculty_id], token, relation_property_id)
                    if update_response is not None:
                        # print(f"Update response: {update_response}")
                        new_match_count += 1
                        existing_relations.add(relation_key)
                        save_relation(relation_key)
                        time.sleep(1)
                    else:
                        print(f"Skipping match due to update error")
                else:
                    # print(f"Relation already exists between Program {program['id']} and Faculty {faculty_id}")
                    pass
        else:
            print(f"Program {program['id']} does not have a valid email")

    # Print the final count of new matches
    print(f"Total new matches: {new_match_count}")

    end_time = datetime.now()
    print(f"Script ended at: {end_time}")
    print(f"Total execution time: {end_time - start_time}")

if __name__ == "__main__":
    main()
