#trade_etl.py
from urllib import response
import pandas as pd
from pathlib import Path
from sqlalchemy import Table, MetaData
from logging import exception
from marketstack_etl.connectors.trade_api import MarketstackApiClient
from marketstack_etl.connectors.postgresql import PostgreSqlClient
#api_key = "46a991bb3e5569e92abdabcfe4070214"

def extract_symbols(server_name,database_name,username,password,port=5432) ->pd.DataFrame:
    client = PostgreSqlClient(
        server_name=server_name,
        database_name=database_name,
        username=username,
        password=password,
        port=port
    )
    df_symbols = client.read_table("symbols")
    print(df_symbols)
    return df_symbols

def extract_trade(marketstack_api_client: MarketstackApiClient, df_symbols: pd.DataFrame) -> pd.DataFrame:

    #df_symbol = pd.read_csv(symbol_path)
    trade_data = []
    for symbol in df_symbols["symbol"]:
        response = marketstack_api_client.get_trade(symbol=symbol)
        trade_data.extend(response["data"])
    df_trade = pd.DataFrame(trade_data)
    print(df_trade)
    return df_trade
    
def transform(df_trade: pd.DataFrame) -> pd.DataFrame:
    pd.options.mode.chained_assignment = 'warn' 
    #select colums to keep
    df_trade = df_trade[["open","high","low","close","volume","name","exchange_code","asset_type","price_currency","symbol","date"]]
    #format date column
    df_trade["date"] = pd.to_datetime(df_trade["date"])
    #drop rows with missing price and volume
    df_trade = df_trade.dropna(subset=["close","volume"])
    #sort by symbol and date 
    df_trade = df_trade.sort_values(by=["symbol", "date"])
    #remove duplicate rows
    df_trade = df_trade.drop_duplicates()
    print(df_trade)
    return df_trade

# def load():
#     return none
