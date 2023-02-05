from _utils import read_config, validate_exchange, get_base_url, get_candlestick_url
import requests
import pandas as pd


def get_valid_exchanges() -> list:
    '''
    A function that returns a list of crypto exchanges that are supported by this code.
    In other words, you can pull trading data for only the exchanges that are in this list.

    Parameters
    ----------
    None

    Returns
    -------
    A list of supported exchanges

    Examples
    --------
    >>> get_valid_exchanges()
    ['coinbase']
    '''

    valid_exchanges = read_config().keys()

    return list(valid_exchanges)


def get_all_tickers(exchange: str) -> pd.DataFrame:
    '''
    A function that returns a dataframe of all tickers from the exchange.

    Parameters
    ----------
    exchange : str
        The name of the crypto exchange to get tickers for

    Returns
    -------
    A list of tickers for the given exchange

    Examples
    --------
    >>> get_ticker_ids('coinbase')
    ['1INCH-BTC', '1INCH-EUR', '1INCH-GBP', '1INCH-USD', ...]
    '''

    exchange = validate_exchange(exchange)

    url = get_base_url(exchange)
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f'Failed to retrieve data from {exchange} api')

    df = pd.DataFrame(data=response.json())

    return df


class Trades:
    '''
    A class for pulling crypto trading data and running backtesting strategies.

    Parameters
    ----------
    exchange : str
        The name of the crypto exchange to get data for

    Returns
    -------
    **Attributes**
    exchange : str
        The name of the crypto exchange to get data for

    Examples
    --------
    >>> trades = Trades('coinbase')
    '''

    def __init__(self,
        exchange: str,
        ticker_id: str) -> None:

        exchange = exchange.lower() #TODO move the .lower() method to utils
        ticker_id = ticker_id.upper() #TODO move the ticker_id parameter to the get_data method

        if not validate_exchange(exchange):
            raise ValueError('Please provide a valid crypto exchange. Run get_valid_exchanges() for a full list of exchanges')

        valid_ids = get_all_tickers(exchange)['id'].values
        if ticker_id not in valid_ids:
            raise ValueError('Please provide a valid ticker id. Run get_ticker_ids() for a full list of tickers')

        self.exchange = exchange
        self.ticker_id = ticker_id

    def get_data(self,
        start_date: str,
        end_date: str,
        start_time: str = '00:00:00',
        end_time: str = '23:59:59',
        time_interval: str = '1_day') -> pd.DataFrame:
        '''
        A method that returns a pandas dataframe with trading data from the exchange.

        Columns include time, low, high, open, close, and volume.
        Volume is expressed as the number of coins traded.

        Parameters
        ----------
        start_date: str
            The start date for the trading data provided as 'YYYY-MM-DD'
        end_date: str
            The end date (inclusive) for the trading data provided as 'YYYY-MM-DD'
        start_time: str
            The start time for the trading data provided as 'HH:MM:SS'.
            Default is '00:00:00' and is only needed when specifying
            time intervals less than 1 day.
        end_time: str
            The end time for the trading data provided as 'HH:MM:SS'.
            Default is '23:59:59' and is only needed when specifying
            time intervals less than 1 day.
        time_interval: str
            The time interval for the trading data. Valid arguments include:
            '1_minute', '5_minute', '15_minute', '1_hour', '6_hour', '1_day'.

        Returns
        -------
        A pandas dataframe with trading data.

        Examples
        --------
        >>> trades = Trades('coinbase', 'BTC-USD')
        >>> trades.get_data('2022-10-01', '2022-10-03')
            time	low	high	open	close	volume
        0	2022-10-01	19160.00	19486.43	19423.57	19315.27	7337.455956
        1	2022-10-02	18923.81	19398.94	19315.26	19059.17	12951.424045
        2	2022-10-03	18958.29	19717.67	19059.10	19633.46	28571.640187
        '''

        #TODO need to provide support for indicators. It will ideally get added to this function since we want to pull the data once

        if not type(time_interval) is str:
            raise TypeError("Please provide a string to time_interval")
            
        start_timestamp = start_date + ' ' + start_time
        end_timestamp = end_date + ' ' + end_time

        time_intervals = {
            '1_minute': 60,
            '5_minute': 300,
            '15_minute': 900,
            '1_hour': 3600,
            '6_hour': 21600,
            '1_day': 86400
        }

        if time_interval not in time_intervals.keys():
            msg = (
                "Please provide a valid time interval. "
                "Valid intervals are: "
            )
            raise KeyError(msg + str(list(time_intervals.keys())))

        url = get_candlestick_url(self.exchange)
        url = url.format(ticker_id=self.ticker_id)

        headers = {"accept": "application/json"}
        
        payload = {
            'granularity': time_intervals[time_interval],
            'start': start_timestamp,
            'end': end_timestamp
            }

        response = requests.get(url, headers=headers, params=payload)

        df = pd.DataFrame(data=response.json(), columns=['time', 'low', 'high', 'open', 'close', 'volume'])

        # converting unix time to datetime
        df['time'] = pd.to_datetime(df['time'], unit='s') #TODO need to conver to local time of the user

        # sorting by time
        df.sort_values(by=['time'], inplace=True, ignore_index=True)

        return df


        
