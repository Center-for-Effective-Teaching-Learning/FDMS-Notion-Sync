# FDMS-Notion-Sync

## Overview

These scripts facilitate the synchronization of faculty programming records between a MySQL database and a Notion database. All combined, they perform the following tasks:

1. Fetches records from MySQL.
2. Fetches existing records from Notion.
3. Compares records and updates Notion if changes are detected.
4. Inserts new records into Notion if they are not present.
5. Optionally sends a summary email of updates.
6. Link records based on Notion relationship
