# Marketstack ETL 
## Project Overview

This project impliments an **ETL pipeline** that extracts trade data from **Marketstack API**.Transforms using Pandas and loads into postgres database.
The pipeline supports both **full** and **incremental** load.
## Tech Stack 

| Component	| Technology Used |
|----------------|-----------------------------|
| Language/Libraries 	| Python 3.10.18, Pandas, SQLAlchemy |
| API	| [Marketstack API](https://marketstack.com/documentation_v2) |
| Database	| PostgreSQL |
| Logging 	| Loguru |
| Containerization	| Docker |
| Version Control	| Git + Github |

## ETL Flow 

**Extract:**
Connects to the Marketstack API to retrieve End-of-Day (EOD) trading data. The pipeline supports both full and incremental loads — a full extract retrieves all available historical data up to the current execution date, while an incremental load fetches only the new data added since the last successful extraction.

**Transform:**
Cleans and standardizes the raw API response into a structured DataFrame.Ensures correct data types and date formats.Filters unnecessary fields and renames columns for consistency.

**Load:**
Writes data to a PostgreSQL target table using SQLAlchemy.Supports incremental and upsert logic.

## Folder Structure 

## Configuration

<pre>
#Create a .env file in the project root
ACCESS_KEY="Your API KEY"
SOURCE_DATABASE_NAME="source_database_name"
SOURCE_DB_USERNAME="username"
SOURCE_DB_PASSWORD="Password"
SOURCE_SERVER_NAME="localhost/RDS endpoint"
SOURCE_PORT="5432"
TARGET_DB_USERNAME="username"
TARGET_DB_PASSWORD="password"
TARGET_SERVER_NAME="localhost/RDS endpoint"
TARGET_DATABASE_NAME="target_database_name"
TARGET_PORT="5432"
TARGET_TABLE_NAME="target_table_name"
</pre>
## Pipeline Run
<pre>
#Run ETL pipeline:
python -m marketstack_etl.trade_pipeline
</pre>
## Docker Run
<pre>
docker build -t marketstack-etl .
docker run --env-file .env marketstack-etl
</pre>
## Testing
<pre>
pytest tests/
</pre>
## Logging 

