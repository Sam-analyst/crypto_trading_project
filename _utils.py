from datetime import datetime, timedelta
import pandas as pd
import requests
import yaml
import math

def read_config() -> dict:
    '''
    A function that reads the config.yaml file within the current directory 
    and returns the contents as a nested dictionary.
    '''

    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    return config


def validate_exchange(exchange: str) -> str:
    '''
    A function that determines if the provided exchange is one of 
    the supported exchanges in this code. If so, it returns the
    exchange in the proper format.

    Parameters
    ----------
    exchange : str
        The name of the crypto exchange to get tickers for

    Returns
    -------
    exchange

    Examples
    --------
    >>> validate_exchange('COINBASE')
    'coinbase'
    '''

    # first making sure a string was provided
    if not isinstance(exchange, str):
        raise TypeError('Please provide a string')
    
    # lowercaseing just in case
    exchange = exchange.lower()

    # grabbing the key values from the read_config dictionary, which should be exchanges
    valid_exchanges = read_config().keys()

    # raising an error if exchange not in config.yaml
    if exchange not in valid_exchanges:
        raise ValueError('Please provide a valid crypto exchange. Run get_valid_exchanges() for a list of supported exchanges')

    return exchange


def get_base_url(exchange: str) -> str:
    '''
    A function that returns the base api url from the config.yaml file.
    This url is used when data on all available trading pairs is requested.

    Parameters
    ----------
    exchange : str
        The name of the crypto exchange

    Returns
    -------
    A string of the base url

    Examples
    --------
    >>> get_base_url('coinbase')
    'https://api.exchange.coinbase.com/products/'
    '''

    # reading the config file, filtering for the given exchange, and then selecting base_url
    base_url = read_config()[exchange]['base_url']
    
    return base_url


def get_candlestick_url(exchange: str) -> str:
    '''
    A function that returns the url needed to get candlestick data
    from the exchange.

    Parameters
    ----------
    exchange : str
        The name of the crypto exchange

    Returns
    -------
    A string of the candlestick url

    Examples
    --------
    >>> get_candlestick_url('coinbase')
    'https://api.exchange.coinbase.com/products/{ticker_id}/candles'
    '''

    # reading the config file, filtering for the given exchange, and then selecting candlestick_url
    candlestick_url = read_config()[exchange]['candlestick_url']
    
    return candlestick_url


def get_time_intervals(exchange: str) -> dict:
    '''
    A function that returns the supported time intervals
    by the exchange api.

    Parameters
    ----------
    exchange : str
        The name of the crypto exchange

    Returns
    -------
    A dictionary of supported time intervals where the key are the
    time intervals and the values are the time intervals expressed
    as seconds.

    Examples
    --------
    >>> get_time_intervals('coinbase')
    {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '6h': 21600, '1d': 86400}
    '''

    # reading the config file, filtering for the given exchange, and then selecting time_intervals
    time_intervals = read_config()[exchange]['time_intervals']
    
    return time_intervals


def get_row_count(start_datetime: datetime,
                  end_datetime: datetime,
                  time_interval: str,
                  time_interval_in_seconds: int,
                  ) -> int:
    '''
    This function takes a start and end datetime and calculates how many rows would
    be returned from the exchange given a time interval.

    Parameters
    ----------
    start_datetime : datetime
        The start datetime object.
    end_datetime : datetime
        The end datetime object.
    time_interval : str
        The time interval for the trading data.
    time_interval_in_seconds : int
        The time interval expressed as seconds.

    Returns
    -------
    An integer that represents how many rows in a dataframe will be returned from the api.
    '''

    # first calculating the difference in time
    diff = end_datetime - start_datetime

    # then converting to seconds
    diff_seconds = diff.total_seconds()

    # dividing the difference in seconds by the interval
    number_of_rows = diff_seconds/time_interval_in_seconds

    # now making adjustments to the number_of_rows since the end date is inclusive...
    # if we simply take the difference in times above, our counts will be off by 1
    # so we need to make adjustments based on the times provided so an accurate row
    # count can be provided

    # calculating the row count on the daily tf is easy since the passed in times are being overriden
    # since the end time is 23:59:59, the difference will be a little bit less than the correct
    # row count, so all we need to do is run a ceil function. round would work here too I believe
    if time_interval == '1d':
        number_of_rows = math.ceil(number_of_rows)

    # for hourly time frames, if the minute is greater than 1, then we can just run the same logic
    # as above. However, if it is equal to 0, then we run into the issue I referenced above where
    # taking the difference will product a row count that is one less than the actual row count
    # since the api end date is inclusive. To fix this, all we'll need to do is add 1.
    elif time_interval == '1h' or time_interval == '6h':
        if end_datetime.minute != 0:
            number_of_rows = math.ceil(number_of_rows)
        else:
            number_of_rows = round(number_of_rows) + 1

    # same logic as the hourly time frame as above, but looking at the seconds data.
    elif time_interval == '15m' or time_interval == '5m' or time_interval == '1m':
        if end_datetime.second != 0:
            number_of_rows = math.ceil(number_of_rows)
        else:
            number_of_rows = round(number_of_rows) + 1

    return number_of_rows

def get_date_ranges(start_date_dt,
                    final_end_date,
                    time_interval_in_seconds
                    ) -> list:
    
    '''
    Since the coinbase API can only handle 300 records at a time, this function
    was created to break the time range into smaller increments so that
    multiple requests can be made to the API.

    Returns
    -------
    List of lists where the first value in the list is the start range and
    the second value is the end range.
    '''

    temp_end_date = start_date_dt + timedelta(seconds=time_interval_in_seconds * 290) # using 290 to be safe

    date_ranges = []
    if temp_end_date >= final_end_date:
        date_ranges.append([start_date_dt, final_end_date])
    else:
        date_ranges.append([start_date_dt, temp_end_date])

    while temp_end_date < final_end_date:
        start_date_dt = temp_end_date + timedelta(seconds=time_interval_in_seconds) # new start date is the first candle after the last date range end date
        temp_end_date = start_date_dt + timedelta(seconds=time_interval_in_seconds * 290)

        if temp_end_date > final_end_date:
            temp_end_date = final_end_date

        date_ranges.append([start_date_dt, temp_end_date])

    return date_ranges

def get_requests(url: str,
                 payload: dict = None,
                 columns: list = None) -> pd.DataFrame:
    '''
    A function that handles sending requests to the API's and 
    returns the data as a pandas dataframe.

    Parameters
    ----------
    url : str
        The api url
    payload : dict, optional
        The payload that goes along with the request. Default is none
    columns : list, optional
        The list of columns to rename in the pandas dataframe

    Returns
    -------
    A pandas dataframe containing the information returned from the api.
    '''

    # sending the request
    response = requests.get(url, params=payload)

    # raising an error if something went wrong with the request
    if response.status_code != 200:
        raise Exception(f'Failed to retrieve data from the api')

    # throwing the data into a pandas dataframe and renaming the columns if provided
    # if columns is None, the column names returned from the api will remain intact.
    df = pd.DataFrame(data=response.json(), columns=columns)

    return df