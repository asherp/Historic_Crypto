# -*- coding: utf-8 -*-

import pandas as pd
import requests
import json
import sys


class LiveCryptoData(object):
    """
    This class provides methods for obtaining live Cryptocurrency price data,
    including the Bid/Ask spread from the CoinBase Pro API.

    :param: ticker: information for which the user would like to return. (str)
    :param: verbose: print progress during extraction, default = True (bool)
    :returns: response_data: a Pandas DataFrame which contains the requested cryptocurrency data. (pd.DataFrame)
    """
    def __init__(self,
                 ticker,
                 verbose=True):

        if verbose:
            print("Checking inputs...")

        if not isinstance(ticker, str):
            raise TypeError("The 'ticker' argument must be a string object.")
        if not isinstance(verbose, (bool, type(None))):
            raise TypeError("The 'verbose' argument must be a boolean or None type.")

        self.verbose = verbose
        self.ticker = ticker

    def _ticker_checker(self):
        """This helper function checks if the ticker is available on the CoinBase Pro API."""
        if self.verbose:
            print("Checking if the user-supplied ticker is available on the CoinBase Pro API...")

        tkr_response = requests.get("https://api.pro.coinbase.com/products")
        if tkr_response.status_code in [200, 201, 202, 203, 204]:
            if self.verbose:
                print('Connected to the CoinBase Pro API.')
            response_data = pd.json_normalize(json.loads(tkr_response.text))
            ticker_list = response_data["id"].tolist()

        # Handle malformed request or client errors
        elif tkr_response.status_code in [400, 401, 404]:
            if self.verbose:
                print(f"Status Code: {tkr_response.status_code}, malformed request to the CoinBase Pro API.")
            raise requests.HTTPError(f"Malformed request with status code {tkr_response.status_code}.")

        # Handle server errors or forbidden access
        elif tkr_response.status_code in [403, 500, 501]:
            if self.verbose:
                print(f"Status Code: {tkr_response.status_code}, could not connect to the CoinBase Pro API.")
            raise requests.ConnectionError(f"Connection failed with status code {tkr_response.status_code}.")

        # Handle any other unknown errors
        else:
            if self.verbose:
                print(f"Status Code: {tkr_response.status_code}, error in connecting to the CoinBase Pro API.")
            raise requests.RequestException(f"Unknown error with status code {tkr_response.status_code}.")

        # Check if the ticker is available
        if self.ticker in ticker_list:
            if self.verbose:
                print(f"Ticker '{self.ticker}' found at the CoinBase Pro API, continuing to extraction.")
        else:
            raise ValueError(f"Ticker '{self.ticker}' not available through CoinBase Pro API. Please use the Cryptocurrencies class to identify the correct ticker.")

    def return_data(self):
        """This function returns the desired output."""
        if self.verbose:
            print(f"Collecting data for '{self.ticker}'")

        self._ticker_checker()
        response = requests.get(f"https://api.pro.coinbase.com/products/{self.ticker}/ticker")

        if response.status_code in [200, 201, 202, 203, 204]:
            if self.verbose:
                print(f"Status Code: {response.status_code}, successful API call.")
            response_data = pd.json_normalize(json.loads(response.text))
            response_data["time"] = pd.to_datetime(response_data["time"])
            response_data.set_index("time", drop=True, inplace=True)
            return response_data

        # Handle malformed request or client errors
        elif response.status_code in [400, 401, 404]:
            if self.verbose:
                print(f"Status Code: {response.status_code}, malformed request to the CoinBase Pro API.")
            raise requests.HTTPError(f"Malformed request with status code {response.status_code}.")

        # Handle server errors or forbidden access
        elif response.status_code in [403, 500, 501]:
            if self.verbose:
                print(f"Status Code: {response.status_code}, error in connecting to the CoinBase Pro API.")
            raise requests.ConnectionError(f"Connection failed with status code {response.status_code}.")

        # Handle any other unknown errors
        else:
            if self.verbose:
                print(f"Status Code: {response.status_code}, error in connecting to the CoinBase Pro API.")
            raise requests.RequestException(f"Unknown error with status code {response.status_code}.")


# new = LiveCryptoData('BTC-USD').return_data()
# print(new)
