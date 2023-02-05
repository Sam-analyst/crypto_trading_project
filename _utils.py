import yaml

def read_config() -> dict:
    '''
    A function that reads the config.yaml file within the current directory 
    and returns the contents as a nested dictionary.
    '''

    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    return config


def validate_exchange(exchange: str) -> bool:
    '''
    A function that determines if the given exchange is one of the supported
    exchanges in this code.

    Parameters
    ----------
    exchange : str
        The name of the crypto exchange to get tickers for

    Returns
    -------
    True/False

    Examples
    --------
    >>> validate_exchange('coinbase')
    True
    
    >>> validate_exchange('something')
    False
    '''

    if not isinstance(exchange, str):
        raise TypeError('Please provide a string')
    
    exchange = exchange.lower()

    valid_exchanges = read_config().keys()

    if exchange not in valid_exchanges:
        raise ValueError('Please provide a valid crypto exchange')

    return exchange

def get_base_url(exchange: str):
    '''
    A function that returns the base api url from the config.yaml file.
    This url is used when data for all tickers is requested.
    For example, getting all valid ticker IDs on the exchange.

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


