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
.
├── Dockerfile
├── images
│   └── architecture-diagram.png
├── logs
│   ├── app_2025-10-13_06.jsonl
│   ├── app_2025-10-13_07.jsonl
│   └── app_2025-10-13_22.jsonl
├── marketstack_etl
│   ├── __init__.py
│   ├── assets
│   ├── connectors
│   ├── data
│   └── trade_pipeline.py
├── marketstack_etl_tests
│   ├── __init__.py
│   └── connectors
├── project-plan.md
├── README.md
└── requirements.txt

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
The logs are printed to the console.The logs include timestamp, module details and the log message.
Sample log:
<pre>
2025-10-14 01:58:13.001 | INFO     | __main__:<module>:33 | Pipeline Starting {}
2025-10-14 01:58:13.008 | INFO     | marketstack_etl.connectors.postgresql:__init__:58 | Database connection stablished {}
2025-10-14 01:58:13.055 | INFO     | marketstack_etl.assets.trade_etl:extract_trade:72 | Target table stock_information is empty. Performing full extract for all symbols. {}
2025-10-14 01:58:13.055 | INFO     | marketstack_etl.assets.trade_etl:extract_trade:91 | Fetching data for symbol AAPL from beginning to 2025-10-13 {}
2025-10-14 01:58:13.380 | INFO     | marketstack_etl.assets.trade_etl:extract_trade:98 | Fetched 100 trades for AAPL. {}
2025-10-14 01:58:13.381 | INFO     | marketstack_etl.assets.trade_etl:extract_trade:91 | Fetching data for symbol MSFT from beginning to 2025-10-13 {}
2025-10-14 01:58:13.687 | INFO     | marketstack_etl.assets.trade_etl:extract_trade:98 | Fetched 100 trades for MSFT. {}
2025-10-14 01:58:13.688 | INFO     | marketstack_etl.assets.trade_etl:extract_trade:91 | Fetching data for symbol GOOGL from beginning to 2025-10-13 {}
2025-10-14 01:58:13.978 | INFO     | marketstack_etl.assets.trade_etl:extract_trade:98 | Fetched 100 trades for GOOGL. {}
2025-10-14 01:58:13.982 | INFO     | marketstack_etl.assets.trade_etl:transform:130 | Starting transformation on 300 trade records. {}
2025-10-14 01:58:13.996 | INFO     | marketstack_etl.assets.trade_etl:transform:158 | Transformation complete. returning 300 cleaned trade records. {}
2025-10-14 01:58:14.000 | INFO     | __main__:<module>:78 | Load Process starting {}
2025-10-14 01:58:14.096 | INFO     | __main__:<module>:87 | Data Loading Process Finished! {}
</pre>
