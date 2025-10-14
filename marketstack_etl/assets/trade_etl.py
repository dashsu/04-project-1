"""
    extract_symbols: Extract stock symbols from database table
    extract_trade: Extract trade data from Marketstack API for the symbols fetched from database
    transform: Clean and transform trade data extracted from API
    load: Load transformed data into PostgreSQL with upsert capability
    _create_table: Helper function to create database tables

"""

import os
from pathlib import Path
from datetime import datetime, timedelta
import logging

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Table, MetaData, Column, inspect
from sqlalchemy.engine import URL, Engine
from sqlalchemy.dialects import postgresql  # only if you use PostgreSQL-specific types
from jinja2 import Environment, FileSystemLoader, Template

from marketstack_etl.assets.helpers import setup_logger
from marketstack_etl.connectors.trade_api import MarketstackApiClient

load_dotenv()
def extract_symbols(client) -> pd.DataFrame:
    """
    Extract stock symbols from a database table.
    
    Args:
        client: Database client object with read_table method
        
    Returns:
        pd.DataFrame: DataFrame containing stock symbols from the 'symbols' table
    """
    df_symbols = client.read_table("symbols")
    return df_symbols

logger=setup_logger()

def extract_trade(marketstack_api_client: MarketstackApiClient, 
                df_symbols: pd.DataFrame,
                target_engine,
                target_table
                ) -> pd.DataFrame:
    """
    Extract trade data from Marketstack API for given symbols with incremental loading.
    
    Args:
        marketstack_api_client (MarketstackApiClient): API client for Marketstack
        df_symbols (pd.DataFrame): DataFrame containing stock symbols to process
        target_engine: SQLAlchemy engine for the target database
        target_table (str): Name of the target table to check for existing data
        
    Returns:
        pd.DataFrame: DataFrame containing extracted trade data with columns:
            - open, high, low, close, volume, name, exchange_code, asset_type,
              price_currency, symbol, date
    """

# Check if the taget table exist in postgres

    inspector = inspect(target_engine)
    if not inspector.has_table(target_table):
        logger.info(f"Target table {target_table} not found.Please create the target table.")
        symbol_max_dates = {}
    else:
        query = f"Select symbol, max(date) as max_date from {target_table} group by symbol"
        df_symbol_max = pd.read_sql(query, target_engine)

        if df_symbol_max.empty:
            logger.info(f"Target table {target_table} is empty. Performing full extract for all symbols.")
            symbol_max_dates= {}
        else:
            symbol_max_dates = (df_symbol_max.set_index("symbol")["max_date"].to_dict())
            

    trade_data = []

    for symbol in df_symbols["symbol"]:
        if symbol in symbol_max_dates and pd.notnull(symbol_max_dates[symbol]):
            date_from = (pd.to_datetime(symbol_max_dates[symbol])+pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            date_from = None
        date_to = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        if date_from and date_from > date_to:
            logger.info(f"Skipping {symbol}: the extraction is already up to date.")
            continue

        logger.info(f"Fetching data for symbol {symbol} from {date_from or 'beginning'} to {date_to}")
        
        try:
            response = marketstack_api_client.get_trade(symbol=symbol, date_from=date_from, date_to=date_to)

            if response.get("data"):
                trade_data.extend(response["data"])
                logger.info(f"Fetched {len(response['data'])} trades for {symbol}.")
            else:
                logger.info(f"No data returned for {symbol}.")
        except Exception as e:
            logger.error(f"Extract failed for {symbol} :{e}")
            continue

    df_trade = pd.DataFrame(trade_data)
    if df_trade.empty:
        logger.info(f"No trade data returned for any symbol.")
        return pd.DataFrame()
    #print (df_trade)
    return df_trade
 

def transform(df_trade: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and clean trade data before loading into databse
    
    Args:
        df_trade (pd.DataFrame): Raw trade data from API extraction
        
    Returns:
        pd.DataFrame: Cleaned and transformed data ready for database loading.
                     Returns empty DataFrame if no data provided.
    """
    #if no new records extracted

    if df_trade.empty:
        logger.info(f"No new trade records available for transformation.")
        return pd.DataFrame()
    
    logger.info(f"Starting transformation on {len(df_trade)} trade records.")

    # Normalize, filter, and type-cast the trade dataset before loading
    pd.options.mode.chained_assignment = None

    # Keep only the columns needed to load
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
    logger.info(f"Transformation complete. returning {len(df_trade)} cleaned trade records." )
    return df_trade

def _create_table(table_name: str, metadata: MetaData, engine: Engine):
    """
    Create a database table by cloning columns from existing metadata
    
    Args:
        table_name (str): Name of the table to create
        metadata (MetaData): SQLAlchemy metadata containing table definitions
        engine (Engine): SQLAlchemy engine for the target database
        
    Returns:
        MetaData: New metadata object containing the created table definition
    """
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
    """
    Load data into PostgreSQL database with upsert capability
    
    Args:
        data (pd.DataFrame): Transformed data to load into the database
        table_name (str): Name of the target table
        engine (Engine): SQLAlchemy engine for the target database
        source_metadata (MetaData): Metadata containing table schema definition
        chunksize (int, optional): Number of rows to process per batch. Defaults to 1000.
        
    Raises:
        Exception: If database operation fails during loading
        
    """
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
        logger.info(f"Failed to load data into the database")
        logger.info(e)