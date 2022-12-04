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

    valid_exchanges = read_config().keys()

    return exchange in valid_exchanges


def get_base_url(exchange: str):
    '''
    A function that returns the base api url from the config.yaml file.

    Parameters
    ----------
    exchange : str
        The name of the crypto exchange to get the api url for

    Returns
    -------
    A string of the base api url

    Examples
    --------
    >>> get_base_url('coinbase')
    'https://api.exchange.coinbase.com/products/'
    '''

    url = read_config()[exchange]['base_url']
    
    return url


