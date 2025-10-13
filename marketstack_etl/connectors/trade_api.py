"""
Marketstack API Client to connect to  Marketstack API 
EOD endpoint and return the trade data response.

Classes:
    MarketstackApiClient: Client for Marketstack API

"""

import requests
from marketstack_etl.assets.helpers import setup_logger

logger = setup_logger()

class MarketstackApiClient:
    """
    Marketstack API Client
    
    A client class for interacting with the Marketstack API to retrieve
    end-of-day stock market data. Handles authentication and API requests.
    
    Attributes:
        base_url (str): Base URL for the Marketstack API endpoint
        access_key (str): API access key for authentication
    """
    
    def __init__(self, access_key: str):
        """
        Initialize Marketstack API client with access key.
        
        Args:
            access_key (str): API access key for Marketstack authentication
        """
        self.base_url = "http://api.marketstack.com/v2/eod"
        if access_key is None:
            logger.info("No API key provided")
            raise Exception("API key cannot be set to None.")
        self.access_key = access_key

    def get_trade(self, symbol: str, date_from: str = None, date_to: str = None) -> dict:
        """
        Retrieve end-of-day trade data for a specific stock symbol.
        
        Args:
            symbol (str): Stock symbol to retrieve data for (e.g., 'AAPL', 'MSFT')
            date_from (str, optional): Start date in YYYY-MM-DD format. If None, 
                                     retrieves all available data
            date_to (str, optional): End date in YYYY-MM-DD format. If None, 
                                   uses current date
                                   
        Returns:
            dict: response containing trade data in JSON format:            
        Raises:
            Exception: If API request fails with status code other than 200
        """
        params ={"symbols" : symbol,
                 "access_key" : self.access_key
        }        
        if date_from: params["date_from"] = date_from
        if date_to: params["date_to"] = date_to
        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            logger.info(f"Status code:{response.status_code}")
            logger.info(f"Response: {response.text}")
            raise Exception("Data extraction failed.Please check if the API limit has reached.")