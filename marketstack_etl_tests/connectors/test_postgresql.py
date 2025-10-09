from marketstack_etl.connectors.postgresql import PostgreSqlClient
from dotenv import load_dotenv
import os
import pytest
import pandas as pd


@pytest.fixture
def setup_postgresql_client():
    load_dotenv()

    # Source Database variables
    SOURCE_DATABASE_NAME = os.environ.get("SOURCE_DATABASE_NAME")
    SOURCE_SERVER_NAME = os.environ.get("SOURCE_SERVER_NAME")
    SOURCE_DB_USERNAME = os.environ.get("SOURCE_DB_USERNAME")
    SOURCE_DB_PASSWORD = os.environ.get("SOURCE_DB_PASSWORD")
    SOURCE_PORT = os.environ.get("SOURCE_PORT")

    postgre_client = PostgreSqlClient(SOURCE_SERVER_NAME, SOURCE_DATABASE_NAME, SOURCE_DB_USERNAME, SOURCE_DB_PASSWORD, SOURCE_PORT)
    return postgre_client


def test_read_table(setup_postgresql_client):
    postgresql_client = setup_postgresql_client
    table_name = "tickers"
    result = postgresql_client.read_table(table_name)
    assert len(result) > 0
