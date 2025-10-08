from marketstack_etl.connectors.trade_api import MarketstackApiClient
import pytest
from dotenv import load_dotenv
import os


@pytest.fixture
def setup_marketstackapi_client():
    load_dotenv()

    MARKET_APIKEY = os.environ.get("MARKET_APIKEY")
    marketstackapi = MarketstackApiClient(
        access_key=MARKET_APIKEY
    )
    return marketstackapi


def test_get_trade(setup_marketstackapi_client):
    marketstackapi_client = setup_marketstackapi_client
    symbol_test = "AAPL"
    result = marketstackapi_client.get_trade(symbol_test)
    assert len(result) > 0
