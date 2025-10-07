import requests
from assets.helpers import setup_logger

logger = setup_logger()

class MarketstackApiClient:
    # Class to handle API requests
    def __init__(self,access_key: str):
        self.base_url = "http://api.marketstack.com/v2/eod"
        if access_key is None:
            logger.info("No API key provided")
            raise Exception("API key cannot be set to None.")
        self.access_key = access_key

    def get_trade(self, symbol: str) -> dict:
        # Use the API to get the information
        params ={"symbols" : symbol,"access_key" : self.access_key}
        response = requests.get(f"http://api.marketstack.com/v2/eod", params=params)
        if response.status_code == 200:
            return response.json()
        else:
            logger.info(f"Status code:{response.status_code}")
            logger.info(f"Response: {response.text}")
            logger.info("Data extraction failed.Please check if the API limit has reached.")
            raise Exception("Data extraction failed.Please check if the API limit has reached.")