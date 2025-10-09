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

    def get_trade(self, symbol: str, date_from: str = None, date_to: str = None) -> dict:
        # Use the API to get the information
        params ={"symbols" : symbol,
                 "access_key" : self.access_key
        }        
        if date_from: params["date_from"] = date_from
        if date_to: params["date_to"] = date_to
        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            #print("Status code:", response.status_code)
            #print("Response:", response.text)
            logger.info(f"Status code:{response.status_code}")
            logger.info(f"Response: {response.text}")
            logger.info("Data extraction failed.Please check if the API limit has reached.")
            raise Exception("Data extraction failed.Please check if the API limit has reached.")