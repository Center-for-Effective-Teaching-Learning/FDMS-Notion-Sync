Overview
These scripts facilitate the synchronization of faculty programming records between a MySQL database and a Notion database. All combined, they perform the following tasks:

Fetches records from MySQL.
Fetches existing records from Notion.
Compares records and updates Notion if changes are detected.
Inserts new records into Notion if they are not present.
Optionally sends a summary email of updates.
Link records based on Notion relationship
