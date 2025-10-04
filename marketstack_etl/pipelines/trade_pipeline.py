#trade_pipeline.py
from dotenv import load_dotenv
import os
from marketstack_etl.assets.trade_etl import (extract_symbols,extract_trade, transform)
from marketstack_etl.connectors.trade_api import MarketstackApiClient


if __name__ == "__main__":
    load_dotenv()
    ACCESS_KEY = os.environ.get("ACCESS_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    marketstack_api_client = MarketstackApiClient(access_key=ACCESS_KEY)

    df_symbols = extract_symbols(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT
    )
    print("Symbol extraction from db success")
    df_trade = extract_trade(marketstack_api_client=marketstack_api_client,df_symbols=df_symbols)

    df_transformed = transform(df_trade=df_trade)