Project Overview
This project is ETL pipeline that extracts trade data from Marketstack API.Transforms using Pandas and loads into postgres database.

Tech Stack 
Component	Technology Used
Language/Libraries 	Python 3.10.18, Pandas, SQLAlchemy
API	Marketstack API
Database	PostgreSQL
Logging 	Loguru
Containerization	Docker
Version Control	Git + Github

ETL Flow 
Extract
Connects to the Marketstack API to retrieve End-of-Day (EOD) trading data. The pipeline supports both full and incremental loads — a full extract retrieves all available historical data up to the current execution date, while an incremental load fetches only the new data added since the last successful extraction.
Transform
Cleans and standardizes the raw API response into a structured DataFrame.Ensures correct data types and date formats.Filters unnecessary fields and renames columns for consistency.
Load
Writes data to a PostgreSQL target table using SQLAlchemy.Supports incremental and upsert logic.

Folder Structure 

Configuration
Create a .env file in the project root

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
Pipeline Run
activate virtual environment
conda activate <conda_env_name>
run ETL pipeline
python -m marketstack_etl.trade_pipeline
Docker Run
docker build -t marketstack-etl .
docker run --env-file .env marketstack-etl
Testing
pytest tests/
Logging 

