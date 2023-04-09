from datetime import datetime, timedelta
import pandas as pd
import requests
import yaml
import pytz
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

    if not isinstance(exchange, str):
        raise TypeError('Please provide a string')
    
    exchange = exchange.lower()

    valid_exchanges = read_config().keys()

    if exchange not in valid_exchanges:
        raise ValueError('Please provide a valid crypto exchange. Run get_valid_exchanges() for a list of supported exchanges')

    return exchange


def get_base_url(exchange: str):
    '''
    A function that returns the base api url from the config.yaml file.
    This url is used when data for all tickers is requested.
    For example, getting all ticker ids on the exchange.

    Parameters
    ----------
    exchange : str
        The name of the crypto exchange

    Returns
    -------
    A string of the base url

    Examples
    --------
    >>> get_api_url('coinbase')
    'https://api.exchange.coinbase.com/products/'
    '''

    base_url = read_config()[exchange]['base_url']
    
    return base_url


def get_candlestick_url(exchange: str):
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

    candlestick_url = read_config()[exchange]['candlestick_url']
    
    return candlestick_url


def get_datetime(date: str,
                 time: str,
                 date_format: str = '%Y-%m-%d',
                 time_format: str = '%H:%M:%S',
                 convert_to_utc: bool = False,
                 local_timezone: str = None,
                 ) -> datetime:
    '''
    A function that takes a date and a time and converts it to a datetime
    object. This function also converts to UTC datetime if needed,
    but local_timezone will also need to be given.

    Parameters
    ----------
    date: str
        The date to be converted to datetime. The given date should
        have the format 'YYYY-MM-DD', unless specified otherwise by
        providing a different date_format.
    time: str
        The time to be converted to datetime. The given time should
        have the format 'HH:MM:DD', unless specified otherwise by
        providing a different time_format.
    date_format: str
        The format code of the date provided. The available format codes 
        can be referenced in the datetime module documentation.
    time_format: str
        The format code of the time provided. The available format codes 
        can be referenced in the datetime module documentation.
    local_timezone: str
        The local timezone. See the pytz documentation for available
        timezones.
    '''

    # create datetime object out of provided date
    date = datetime.strptime(date, date_format)

    # create datetime object out of provided time
    time = datetime.strptime(time, time_format)

    # combine date and time to create datetime object with date and time
    dt = datetime.combine(date.date(), time.time())

    if convert_to_utc:
        # get the local timezone
        local_timezone = pytz.timezone(local_timezone)

        # convert the local time to UTC
        dt = local_timezone.localize(dt).astimezone(pytz.utc)

    # convert to correct format
    dt = dt.strftime('%Y-%m-%d %H:%M:%S')

    return dt


def get_row_count(start_datetime: str,
                  end_datetime: str,
                  time_interval: str,
                  time_interval_in_seconds: int,
                  datetime_format: str = '%Y-%m-%d %H:%M:%S',
                  ) -> int:
    '''
    This function takes a start time and end time and calculated how many rows
    would be returned from coinbase.
    '''

    # first, let's parse the given dates into datetime objects
    start = datetime.strptime(start_datetime, datetime_format)
    end = datetime.strptime(end_datetime, datetime_format)

    # now calculating the difference in time
    diff = end - start

    # converting to seconds
    diff_seconds = diff.total_seconds()

    # dividing the difference in seconds by the interval
    number_of_rows = diff_seconds/time_interval_in_seconds

    # now making adjustments to the number_of_rows
    if time_interval == '1d':
        number_of_rows = math.ceil(number_of_rows)

    elif time_interval == '1h' or time_interval == '6h':
        if end.minute != 0:
            number_of_rows = math.ceil(number_of_rows)
        else:
            number_of_rows = round(number_of_rows) + 1

    elif time_interval == '15m' or time_interval == '5m' or time_interval == '1m':
        if end.second != 0:
            number_of_rows = math.ceil(number_of_rows)
        else:
            number_of_rows = round(number_of_rows) + 1

    return number_of_rows

def create_date_ranges(start_date,
                       final_end_date,
                       time_interval_in_seconds,
                       date_format='%Y-%m-%d %H:%M:%S') -> list:
    
    '''
    Since the coinbase API can only handle 300 records at a time, this function
    was created to break the time range into smaller increments so that
    multiple requests can be made to the API.

    Returns
    -------
    List of lists where the first value in the list is the start range and
    the second value is the end range.
    '''

    start_date_dt = datetime.strptime(start_date, date_format)
    final_end_date = datetime.strptime(final_end_date, date_format)
    temp_end_date = start_date_dt + timedelta(seconds=time_interval_in_seconds * 290) # using 290 to be safe

    date_ranges = []
    date_ranges.append([start_date_dt.strftime(date_format), temp_end_date.strftime(date_format)])

    while temp_end_date < final_end_date:
        start_date_dt = temp_end_date + timedelta(seconds=time_interval_in_seconds) # new start date is the first candle after the last date range end date
        temp_end_date = start_date_dt + timedelta(seconds=time_interval_in_seconds * 290)

        if temp_end_date > final_end_date:
            temp_end_date = final_end_date

        date_ranges.append([start_date_dt.strftime(date_format), temp_end_date.strftime(date_format)])

    return date_ranges

def get_requests(url: str,
                 payload: dict = None,
                 columns: list = None) -> pd.DataFrame:
    '''
    A function to handle getting reponses from the exchange api
    '''

    response = requests.get(url, params=payload)

    if response.status_code != 200:
        raise Exception(f'Failed to retrieve data from the api')

    df = pd.DataFrame(data=response.json(), columns=columns)

    return df