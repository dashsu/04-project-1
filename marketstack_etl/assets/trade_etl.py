from urllib import response
import pandas as pd
from pathlib import Path
from sqlalchemy import Table, MetaData
from logging import exception
from datetime import datetime
from assets.helpers import setup_logger
from dotenv import load_dotenv
import os
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, Table, MetaData, Column, inspect
from sqlalchemy.dialects import postgresql
from jinja2 import Environment, FileSystemLoader, Template

logger = setup_logger()

def extract_symbols(client) ->pd.DataFrame:
    # Read the list of tickers from a source client into a DataFrame
    try:
        df_symbols = client.read_table("tickers")
    except Exception as e:
        logger.info("Failed to retrive information from the symbols table")
        logger.info(e)
    return df_symbols

def extract_trade(marketstack_api_client, df_symbols: pd.DataFrame) -> pd.DataFrame:
    # Iterate symbols and pull raw trade payloads from the Marketstack client
    trade_data = []
    logger.info("Data extraction Starting")

    # Get stock information from each symbol in our database
    for symbol in df_symbols["symbols"]:
        response = marketstack_api_client.get_trade(symbol=symbol)
        trade_data.extend(response["data"])

    df_trade = pd.DataFrame(trade_data)

    return df_trade


def transform(df_trade: pd.DataFrame) -> pd.DataFrame:

    logger.info("Data Transformation Starting")
    # Normalize, filter, and type-cast the trade dataset before loading
    pd.options.mode.chained_assignment = 'warn' 

    # Keep only the columns needed downstream
    df_trade = df_trade[["open","high","low","close","volume","name","exchange_code","asset_type","price_currency","symbol","date"]]

    # Parse ISO timestamps to datetime
    df_trade["date"] = pd.to_datetime(df_trade["date"])

    # Ensure price and volume are present
    df_trade = df_trade.dropna(subset=["close","volume"])

    # Order deterministically for idempotent loads
    df_trade = df_trade.sort_values(by=["symbol", "date"])

    # Remove exact duplicates to avoid double inserts
    df_trade = df_trade.drop_duplicates()

    # Stamp ETL metadata for lineage
    df_trade["etl_load_timestamp"] = datetime.now()
    df_trade["data_source"] = "marketstack_api"

    # Cast to DB-friendly types
    df_trade["volume"] = df_trade["volume"].astype('Int64')
    df_trade["date"] = df_trade["date"].astype('str')
    df_trade["etl_load_timestamp"] = df_trade["etl_load_timestamp"].astype('str')

    return df_trade

def _create_table(table_name: str, metadata: MetaData, engine: Engine):
    # Materialize a target table by cloning columns from existing metadata
    existing_table = metadata.tables[table_name]
    new_metadata = MetaData()
    columns = [
        Column(column.name, column.type, primary_key=column.primary_key)
        for column in existing_table.columns
    ]
    new_table = Table(table_name, new_metadata, *columns)
    new_metadata.create_all(bind=engine)
    return new_metadata

def load(data: pd.DataFrame, table_name: str, engine: Engine, source_metadata: MetaData, chunksize: int = 1000):
    # Upsert data in chunks using PostgreSQL ON CONFLICT for primary keys
    target_metadata = _create_table(
        table_name=table_name, metadata=source_metadata, engine=engine
    )
    table = target_metadata.tables[table_name]
    max_length = len(data)
    key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]

    try:
        # Stream rows in batches to control memory and transaction size
        for i in range(0, max_length, chunksize):
            if i + chunksize >= max_length:
                lower_bound = i
                upper_bound = max_length
            else:
                lower_bound = i
                upper_bound = i + chunksize
            insert_statement = postgresql.insert(table).values(
                data[lower_bound:upper_bound]
            )
            # Conflict resolution: update non-key fields when PK already exists
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=key_columns,
                set_={
                    c.key: c for c in insert_statement.excluded if c.key not in key_columns
                },
            )
            # Execute the batch upsert against the target engine
            engine.execute(upsert_statement)
    except Exception as e:
        logger.info("Failed to load data into the database")
        logger.info(e)