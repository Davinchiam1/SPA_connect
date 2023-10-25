import datetime
import json
import pickle
import urllib.parse
import pandas as pd

import requests
from credentials import credentials, proxy


class SPA_requests:
    """Base class for api requests to SPA"""

    def __init__(self, endpoint="https://sellingpartnerapi-na.amazon.com", marketplace_id="ATVPDKIKX0DER"):
        self.access_token = None
        self.endpoint = endpoint
        self.marketplace_id = marketplace_id

    def _autorize(self):

        with open("saved token.pkl", "rb") as file:
            loaded_data = pickle.load(file)

        saved_token = {'access_token': loaded_data['access_token'], 'time': loaded_data['time']}

        time_difference = (datetime.datetime.now() - saved_token['time']).seconds
        if time_difference > 3500:
            token_response = requests.post(
                "https://api.amazon.com/auth/o2/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": credentials["refresh_token"],
                    "client_id": credentials["lwa_app_id"],
                    "client_secret": credentials["lwa_client_secret"],
                },
                proxies=proxy
            )
            access_token = token_response.json()["access_token"]

            saved_token = {'access_token': access_token, 'time': datetime.datetime.now()}

            with open("saved token.pkl", "wb") as file:
                pickle.dump(saved_token, file)

        self.access_token = saved_token['access_token']

    def last_orders_request(self, last_days=10, next_token=None, initial_call=True):
        if initial_call:
            self._autorize()

        request_params = {
            "MarketplaceIds": self.marketplace_id,  # required parameter
            "CreatedAfter": (
                    datetime.datetime.now() - datetime.timedelta(days=last_days)
            ).isoformat(),  # orders created since 30 days ago, the date needs to be in the ISO format
        }

        orders = requests.get(self.endpoint + "/orders/v0/orders" + "?" + urllib.parse.urlencode(request_params),
                              headers={"x-amz-access-token": self.access_token},
                              proxies=proxy)


test = SPA_requests()
