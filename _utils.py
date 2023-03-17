import yaml
from datetime import datetime
import pytz

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
