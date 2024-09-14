# -*- coding: utf-8 -*-

import requests
import json
import sys
import pandas as pd


class Cryptocurrencies(object):
    """
    This class provides methods for finding all available Cryptocurrency products within
    the CoinBase pro API.

    :param: extended_output: displays either a condensed or extended output, Default = False (Bool).
    :param: prints status messages Default = True (Bool).
    :param: search for a specific cryptocurrency string Default = None (str).
    :returns: a Pandas DataFrame containing either the extended or condensed output (DataFrame).
    """
    def __init__(self,
                 extended_output=False,
                 coin_search=None,
                 verbose=True):

        if not all(isinstance(v, (bool, type(None))) for v in [extended_output, verbose]):
            raise TypeError("The 'extended_output' and 'verbose' arguments must either be empty, or boolean types.")
        if not isinstance(coin_search, (str, type(None))):
            raise TypeError("The 'coin_search' argument must either be empty, or a string type.")

        self.extended_output = extended_output
        self.coin_search = coin_search
        self.verbose = verbose

    def find_crypto_pairs(self):
        """This function returns all cryptocurrency pairs available at the CoinBase Pro API."""
        response = requests.get("https://api.pro.coinbase.com/products")
        
        # Check for success status codes
        if response.status_code in [200, 201, 202, 203, 204]:
            if self.verbose:
                print("Connected to the CoinBase Pro API.")
            response_data = pd.json_normalize(json.loads(response.text))

        # Handle malformed request or client errors
        elif response.status_code in [400, 401, 404]:
            if self.verbose:
                print(f"Status Code: {response.status_code}, malformed request to the CoinBase Pro API.")
            raise requests.HTTPError(f"Malformed request with status code {response.status_code}.")

        # Handle server errors or forbidden access
        elif response.status_code in [403, 500, 501]:
            if self.verbose:
                print(f"Status Code: {response.status_code}, could not connect to the CoinBase Pro API.")
            raise requests.ConnectionError(f"Connection failed with status code {response.status_code}.")

        # Handle any other unknown errors
        else:
            if self.verbose:
                print(f"Status Code: {response.status_code}, error in connecting to the CoinBase Pro API.")
            raise requests.RequestException(f"Unknown error with status code {response.status_code}.")

        # Filter based on search term if provided
        if self.coin_search:
            outcome = response_data[response_data['id'].str.contains(self.coin_search)]
            if not outcome.empty:
                if self.verbose:
                    print(f"Found {outcome.shape[0]} instances containing the term {self.coin_search}.")
            else:
                outcome = response_data
                if self.verbose:
                    print("Unable to find specific search term, returning all available data.")
        else:
            outcome = response_data

        # Return extended or condensed output based on the extended_output parameter
        if self.extended_output:
            if self.verbose:
                print(f"Extended Output is {self.extended_output}, returning all data.")
            return outcome
        else:
            if self.verbose:
                print(f"Extended Output is {self.extended_output}, returning reduced dataset.")
            outcome = outcome[['id', 'display_name', 'fx_stablecoin', 'max_slippage_percentage', 'status']]
            return outcome


# data = Cryptocurrencies(coin_search='DOGE', extended_output=False).find_crypto_pairs()
# print(data)
