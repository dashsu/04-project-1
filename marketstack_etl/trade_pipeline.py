"""
Trade Data ETL Pipeline

This is the complete ETL pipeline

The pipeline performs the following steps:
Extract stock symbols from source database
Extract trade data from Marketstack API (with incremental loading)
Transform and clean the data
Load data into target PostgreSQL database with upsert capability

"""

from dotenv import load_dotenv
import os
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, Table, MetaData, Column, inspect
from sqlalchemy.dialects import postgresql
from jinja2 import Environment, FileSystemLoader, Template
import pandas as pd
import json
from loguru import logger

from marketstack_etl.assets.trade_etl import load, extract_symbols, extract_trade, transform
from marketstack_etl.connectors.trade_api import MarketstackApiClient
from marketstack_etl.connectors.postgresql import PostgreSqlClient
from marketstack_etl.assets.helpers import get_schema_metadata
from marketstack_etl.assets.helpers import setup_logger

if __name__ == "__main__":

    logger = setup_logger()
    logger.info("Pipeline Starting")

    # Load enviroment variables
    load_dotenv()

    # Source Database variables
    SOURCE_DATABASE_NAME = os.environ.get("SOURCE_DATABASE_NAME")
    SOURCE_SERVER_NAME = os.environ.get("SOURCE_SERVER_NAME")
    SOURCE_DB_USERNAME = os.environ.get("SOURCE_DB_USERNAME")
    SOURCE_DB_PASSWORD = os.environ.get("SOURCE_DB_PASSWORD")
    SOURCE_PORT = os.environ.get("SOURCE_PORT")

    # Target Database Variables
    TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
    TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
    TARGET_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
    TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
    TARGET_PORT = os.environ.get("TARGET_PORT")

    # API key for Markets API
    ACCESS_KEY = os.environ.get("ACCESS_KEY")

    # Target Table name
    table_name = os.environ.get("TARGET_TABLE_NAME")

    # Create the clients for the connections
    postgre_client = PostgreSqlClient(SOURCE_SERVER_NAME, SOURCE_DATABASE_NAME, SOURCE_DB_USERNAME, SOURCE_DB_PASSWORD, SOURCE_PORT)
    marketstack_client= MarketstackApiClient(ACCESS_KEY)

    target_engine = postgre_client.engine

    # Extract the symbols in our database table
    symbols = extract_symbols(postgre_client)

    # Extract the stock information for the symbols in our database
    stock_information = extract_trade(marketstack_client, symbols,target_engine,table_name)

    # Transform the stock information
    stock_information = transform(stock_information)

    # Transform the Pandas Dataframe into a list of dictionaries
    stock_information = stock_information.to_dict('records')


    # Load the stock data into a table in our database
    logger.info("Load Process starting")
    source_metadata = get_schema_metadata(engine=postgre_client.engine)

    load(
            data=stock_information,
            table_name=table_name,
            engine=postgre_client.engine,
            source_metadata=source_metadata
        )
    logger.info("Data Loading Process Finished!")
    


    
    
    
