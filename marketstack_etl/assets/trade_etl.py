import os
from urllib import response
from pathlib import Path
from datetime import datetime, timedelta
from logging import exception
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column, inspect
from sqlalchemy.engine import URL, Engine
from sqlalchemy.dialects import postgresql
from jinja2 import Environment, FileSystemLoader, Template
from marketstack_etl.connectors.trade_api import MarketstackApiClient

load_dotenv()
# Read the list of symbols from a source client into a DataFrame
def extract_symbols(client) ->pd.DataFrame:
    df_symbols = client.read_table("symbols")
    return df_symbols

# Iterate symbols and pull raw trade payloads from the Marketstack client
   
def extract_trade(marketstack_api_client: MarketstackApiClient, 
                df_symbols: pd.DataFrame,
                target_engine,
                target_table
                ) -> pd.DataFrame:

#df_symbol = pd.read_csv(symbol_path)
#extract incrimentally for existing symbol and full for new sysmols 

#first check if the taget thable exist in postgres

    inspector = inspect(target_engine)
    if not inspector.has_table(target_table):
        print(f"Target table {target_table} not found. Do a full extract for all symbols.")
        symbol_max_dates = {}
    else:
        query = f"Select symbol, max(date) as max_date from {target_table} group by symbol"
        df_symbol_max = pd.read_sql(query, target_engine)

        if not df_symbol_max.empty:
            symbol_max_dates = (df_symbol_max.set_index("symbol")["max_date"].to_dict())
        else:
            symbol_max_dates= {}

    trade_data = []

    for symbol in df_symbols["symbol"]:
        if symbol in symbol_max_dates and pd.notnull(symbol_max_dates[symbol]):
            date_from = (pd.to_datetime(symbol_max_dates[symbol])+pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            date_from = None
        date_to = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        print(f"Fetching data for symbol {symbol} from {date_from} to {date_to}")
        try:
            response = marketstack_api_client.get_trade(symbol=symbol, date_from=date_from, date_to=date_to)
            trade_data.extend(response["data"])
        except Exception as e:
            print(f"exception for{symbol}:{e}")

    df_trade = pd.DataFrame(trade_data)
    print(f"Extracted {len(df_trade)} new rows")
    #print(df_trade.columns)
    return df_trade
 

def transform(df_trade: pd.DataFrame) -> pd.DataFrame:

    #if no new records extracted

    if df_trade.empty:
        print("No new records extracted.")
        return pd.DataFrame()

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

    # Cast to DB-friendly types (nullable Int64 for volume; strings for timestamps)
    df_trade["volume"] = df_trade["volume"].astype('Int64')
    df_trade["date"] = df_trade["date"].astype('str')
    df_trade["etl_load_timestamp"] = df_trade["etl_load_timestamp"].astype('str')

    return df_trade

# Materialize a target table by cloning columns from existing metadata
def _create_table(table_name: str, metadata: MetaData, engine: Engine):
    existing_table = metadata.tables[table_name]
    new_metadata = MetaData()
    columns = [
        Column(column.name, column.type, primary_key=column.primary_key)
        for column in existing_table.columns
    ]
    new_table = Table(table_name, new_metadata, *columns)
    new_metadata.create_all(bind=engine)
    return new_metadata

# Upsert data in chunks using PostgreSQL ON CONFLICT for primary keys
def load(data: pd.DataFrame, table_name: str, engine: Engine, source_metadata: MetaData, chunksize: int = 1000,
):
    target_metadata = _create_table(
        table_name=table_name, metadata=source_metadata, engine=engine
    )
    table = target_metadata.tables[table_name]
    max_length = len(data)
    key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]

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