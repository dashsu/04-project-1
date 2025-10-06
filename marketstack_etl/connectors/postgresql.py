from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
import pandas as pd

class PostgreSqlClient:
    # Client for the PostgreSQL Database
    def __init__(self, server_name: str,database_name: str,username: str,password: str,port=5432):
        self.server_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )
        self.engine = create_engine(connection_url)

    def read_table(self, table_name: str) -> pd.DataFrame:
        # Method to read a table from the DB into a pandas dataframe
        df = pd.read_sql_table(table_name,self.engine)
        return df

