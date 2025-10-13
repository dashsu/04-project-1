"""
PostgreSQL Database Connector

Classes:
    PostgreSqlClient: Client for PostgreSQL database 
"""

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
from marketstack_etl.assets.helpers import setup_logger

logger = setup_logger()

class PostgreSqlClient:
    """
    PostgreSQL Database Client
    
    Attributes:
        server_name (str): Database server hostname or IP address
        database_name (str): Name of the database to connect to
        username (str): Database username for authentication
        password (str): Database password for authentication
        port (int): Database port number (default: 5432)
        engine: SQLAlchemy engine object for database operations
    """
    
    def __init__(self, server_name: str, database_name: str, username: str, password: str, port=5432):
        """
        Initialize PostgreSQL client with connection parameters.
        
        Args:
            server_name (str): Database server hostname or IP address
            database_name (str): Name of the database to connect to
            username (str): Database username for authentication
            password (str): Database password for authentication
            port (int, optional): Database port number. Defaults to 5432.
            
        Raises:
            Exception: If database connection fails
        """
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
        """
        Read data from table to pandas DataFrame.
        
        Args:
            table_name (str): Name of the table to read from
            
        Returns:
            pd.DataFrame: DataFrame containing all data from the specified table
        """
        df = pd.read_sql_table(table_name, self.engine)
        return df