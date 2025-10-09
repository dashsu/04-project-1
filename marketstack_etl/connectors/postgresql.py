from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
from assets.helpers import setup_logger

logger = setup_logger()

class PostgreSqlClient:
    # Client for the PostgreSQL Database
    def __init__(self, server_name: str,database_name: str,username: str,password: str,port=5432):
        self.server_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        try: 
            connection_url = URL.create(
                drivername="postgresql+pg8000",
                username=username,
                password=password,
                host=server_name,
                port=port,
                database=database_name,
            )
            self.engine = create_engine(connection_url)
            logger.info("Database connection stablished")

        except Exception as e:
            logger.info("Error connecting with the Database")
            logger.info(e)
            
        

    def read_table(self, table_name: str) -> pd.DataFrame:
        # Method to read a table from the DB into a pandas dataframe
        df = pd.read_sql_table(table_name,self.engine)
        return df

