import requests

class MarketstackApiClient:
    def __init__(self,access_key: str):
        self.base_url = "http://api.marketstack.com/v2/eod"
        if access_key is None:
            raise Exception("API key cannot be set to None.")
        self.access_key = access_key

    def get_trade(self, symbol: str) -> dict:
        params ={"symbols" : symbol,"access_key" : self.access_key}
        response = requests.get(f"http://api.marketstack.com/v2/eod", params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print("Status code:", response.status_code)
            print("Response:", response.text)
            raise Exception("Data extraction failed.Please check if the API limit has reached.")