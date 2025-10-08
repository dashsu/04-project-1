from dotenv import load_dotenv
import os
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, Table, MetaData, Column, inspect
from sqlalchemy.dialects import postgresql
from jinja2 import Environment, FileSystemLoader, Template
import pandas as pd

from .assets.trade_etl import load, extract_symbols, extract_trade, transform
from .connectors.trade_api import MarketstackApiClient
from .connectors.postgresql import PostgreSqlClient
from .assets.helpers import get_schema_metadata


if __name__ == "__main__":

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
    Postgre_Client = PostgreSqlClient(SOURCE_SERVER_NAME, SOURCE_DATABASE_NAME, SOURCE_DB_USERNAME, SOURCE_DB_PASSWORD, SOURCE_PORT)
    Marketstack_Client= MarketstackApiClient(ACCESS_KEY)

    target_engine = Postgre_Client.engine

    # Extract the symbols in our database table
    symbols = extract_symbols(Postgre_Client)

    # Extract the stock information for the symbols in our database
    stock_information = extract_trade(Marketstack_Client, symbols,target_engine,table_name)

    # Transform the stock information
    stock_information = transform(stock_information)

    # Debug Only
    #stock_information.to_csv("stock_information2.csv", index=False)
    #stock_information = pd.read_csv("stock_information2.csv")

    # Transform the Pandas Dataframe into a list of dictionaries
    stock_information = stock_information.to_dict('records')


    # Load the stock data into a table in our database
    source_metadata = get_schema_metadata(engine=Postgre_Client.engine)

    load(
            data=stock_information,
            table_name=table_name,
            engine=Postgre_Client.engine,
            source_metadata=source_metadata
        )