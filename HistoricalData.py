# -*- coding: utf-8 -*-

import requests
import json
import time
from random import randint
import pandas as pd
import sys
from datetime import datetime, timedelta

# map between seconds and new api string granularity
GRANULARITIES = {
   60: 'ONE_MINUTE',
    300: 'FIVE_MINUTE',
    900: 'FIFTEEN_MINUTE',
    1800: 'THIRTY_MINUTE',
    3600: 'ONE_HOUR',
    7200 : 'TWO_HOUR',
    21600: 'SIX_HOUR',
    86400: 'ONE_DAY'
}


class HistoricalData(object):
    """
    This class provides methods for gathering historical price data of a specified
    Cryptocurrency between user specified time periods. The class utilises the CoinBase Pro
    API to extract historical data, providing a performant method of data extraction.
    
    Please Note that Historical Rate Data may be incomplete as data is not published when no 
    ticks are available (Coinbase Pro API Documentation).
    
    :param: ticker: a singular Cryptocurrency ticker. (str)
    :param: granularity: the price data frequency in seconds, one of: 60, 300, 900, 3600, 21600, 86400. (int)
    :param: start_date: a date string in the format YYYY-MM-DD-HH-MM. (str)
    :param: end_date: a date string in the format YYYY-MM-DD-HH-MM,  Default=Now. (str)
    :param: verbose: printing during extraction, Default=True. (bool)
    :returns: data: a Pandas DataFrame which contains requested cryptocurrency data. (pd.DataFrame)
    """

    def __init__(self,
                 ticker,
                 granularity,
                 start_date,
                 end_date=None,
                 verbose=True):
        if verbose:
            print("Checking input parameters are in the correct format.")
        if not all(isinstance(v, str) for v in [ticker, start_date]):
            raise TypeError("The 'ticker' and 'start_date' arguments must be strings or None types.")
        if not isinstance(end_date, (str, type(None))):
            raise TypeError("The 'end_date' argument must be a string or None type.")
        if not isinstance(verbose, bool):
            raise TypeError("The 'verbose' argument must be a boolean.")
        if isinstance(granularity, int) is False:
            raise TypeError("'granularity' must be an integer object.")
        if granularity not in GRANULARITIES:
            raise ValueError("'granularity' argument must be one of 60, 300, 900, 3600, 21600, 86400 seconds.")

        if not end_date:
            end_date = datetime.today().strftime("%Y-%m-%d-%H-%M")
        else:
            end_date_datetime = datetime.strptime(end_date, '%Y-%m-%d-%H-%M')
            start_date_datetime = datetime.strptime(start_date, '%Y-%m-%d-%H-%M')
            if start_date_datetime >= end_date_datetime:
                raise ValueError("'end_date' argument cannot occur prior to the start_date argument.")

        self.ticker = ticker
        self.granularity = granularity
        self.start_date = start_date
        self.start_date_string = None
        self.end_date = end_date
        self.end_date_string = None
        self.verbose = verbose

    def _ticker_checker(self):
        if self.verbose:
            print("Checking if user supplied ticker is available on the CoinBase Pro API.")

        tkr_response = requests.get("https://api.pro.coinbase.com/products")
        if tkr_response.status_code in [200, 201, 202, 203, 204]:
            if self.verbose:
                print('Connected to the CoinBase Pro API.')
            response_data = pd.json_normalize(json.loads(tkr_response.text))
            ticker_list = response_data["id"].tolist()

        elif tkr_response.status_code in [400, 401, 404]:
            error_msg = f"Status Code: {tkr_response.status_code}, malformed request to the CoinBase Pro API"
            if self.verbose:
                print(error_msg)
            raise requests.HTTPError(error_msg)
        elif tkr_response.status_code in [403, 500, 501]:
            if self.verbose:
                print("Status Code: {}, could not connect to the CoinBase Pro API.".format(tkr_response.status_code))
            raise requests.ConnectionError(f"Could not connect to CoinBase Pro API: {tkr_response.status_code}")
        else:
            if self.verbose:
                print("Status Code: {}, error in connecting to the CoinBase Pro API.".format(tkr_response.status_code))
            raise requests.RequestException(f"Error connecting to CoinBase Pro API: {tkr_response.status_code}")

        if self.ticker in ticker_list:
            if self.verbose:
                print(f"Ticker '{self.ticker}' found at the CoinBase Pro API, continuing to extraction.")
        else:
            raise ValueError(f"Ticker: '{self.ticker}' not available through CoinBase Pro API.")

    def _date_cleaner(self, date_time: (datetime, str)):
        if not isinstance(date_time, (datetime, str)):
            raise TypeError("The 'date_time' argument must be a datetime or string.")
        if isinstance(date_time, str):
            output_date = datetime.strptime(date_time, '%Y-%m-%d-%H-%M').isoformat()
        else:
            output_date = date_time.strftime("%Y-%m-%d, %H:%M:%S")
            output_date = output_date[:10] + 'T' + output_date[12:]
        return output_date

    def retrieve_data(self):
        if self.verbose:
            print("Formatting Dates.")

        self._ticker_checker()
        self.start_date_string = self._date_cleaner(self.start_date)
        self.end_date_string = self._date_cleaner(self.end_date)
        start = datetime.strptime(self.start_date, "%Y-%m-%d-%H-%M")
        end = datetime.strptime(self.end_date, "%Y-%m-%d-%H-%M")
        request_volume = abs((end - start).total_seconds()) / self.granularity
        times = pd.date_range(start, end, request_volume)
        request_volume = len(times)
        if self.verbose:
            print(f'request volume: {request_volume}')
        if request_volume <= 300:
            start_unix = int(start.timestamp())
            end_unix = int(end.timestamp())
            url = f"https://api.coinbase.com/api/v3/brokerage/market/products/{self.ticker}/candles?start={start_unix}&end={end_unix}&granularity={GRANULARITIES[self.granularity]}&limit={request_volume}"

            response = requests.get(url)
            if response.status_code in [200, 201, 202, 203, 204]:
                if self.verbose:
                    print('Retrieved Data from Coinbase Pro API.')
                data = response.json()['candles']
                data = pd.DataFrame(data)
                data.columns = ["time", "low", "high", "open", "close", "volume"]
                data["time"] = pd.to_datetime(data["time"], unit='s')
                data = data[data['time'].between(start, end)]
                data.set_index("time", drop=True, inplace=True)
                data.sort_index(ascending=True, inplace=True)
                data.drop_duplicates(subset=None, keep='first', inplace=True)
                if self.verbose:
                    print('Returning data.')
                return data
            elif response.status_code in [400, 401, 404]:
                if self.verbose:
                    print(f"Status Code: {response.status_code}, malformed request to the CoinBase Pro API.")
                raise requests.HTTPError(f"Malformed request: {response.status_code}. request: {url}")
            elif response.status_code in [403, 500, 501]:
                if self.verbose:
                    print(f"Status Code: {response.status_code}, could not connect to the CoinBase Pro API.")
                raise requests.ConnectionError(f"Could not connect to CoinBase Pro API: {response.status_code}")
            else:
                if self.verbose:
                    print(f"Status Code: {response.status_code}, error in connecting to the CoinBase Pro API.")
                raise requests.RequestException(f"Error connecting to API: {response.status_code}")
        else:
            max_per_mssg = 300
            data = pd.DataFrame()
            for i in range(int(request_volume / max_per_mssg) + 1):
                provisional_start = start + timedelta(0, i * (self.granularity * max_per_mssg))
                provisional_end = start + timedelta(0, (i + 1) * (self.granularity * max_per_mssg))

                if self.verbose:
                    print("Provisional Start: {}".format(provisional_start))
                    print("Provisional End: {}".format(provisional_end))
                url = f"https://api.coinbase.com/api/v3/brokerage/market/products/{self.ticker}/candles?start={provisional_start}&end={provisional_end}&granularity={GRANULARITIES[self.granularity]}&limit={request_volume}"

                response = requests.get(url)

                if response.status_code in [200, 201, 202, 203, 204]:
                    if self.verbose:
                        print(f'Data for chunk {i+1} of {(int(request_volume / max_per_mssg) + 1)} extracted.')
                    dataset = pd.DataFrame(response.json()['candles'])
                    if not dataset.empty:
                        data = data.append(dataset)
                        time.sleep(randint(0, 2))
                    else:
                        print(f"CoinBase Pro API did not have available data for '{self.ticker}' beginning at {self.start_date}. Trying a later date: '{provisional_start}'")
                        time.sleep(randint(0, 2))
                elif response.status_code in [400, 401, 404]:
                    if self.verbose:
                        print(f"Status Code: {response.status_code}, malformed request to the CoinBase Pro API.")
                    raise requests.HTTPError(f"Malformed request: {response.status_code}")
                elif response.status_code in [403, 500, 501]:
                    if self.verbose:
                        print(f"Status Code: {response.status_code}, could not connect to the CoinBase Pro API.")
                    raise requests.ConnectionError(f"Could not connect to CoinBase Pro API: {response.status_code}")
                else:
                    if self.verbose:
                        print(f"Status Code: {response.status_code}, error in connecting to the CoinBase Pro API.")
                    raise requests.RequestException(f"Error connecting to API: {response.status_code}")
            data.columns = ["time", "low", "high", "open", "close", "volume"]
            data["time"] = pd.to_datetime(data["time"], unit='s')
            data = data[data['time'].between(start, end)]
            data.set_index("time", drop=True, inplace=True)
            data.sort_index(ascending=True, inplace=True)
            data.drop_duplicates(subset=None, keep='first', inplace=True)
            return data.astype(float)
