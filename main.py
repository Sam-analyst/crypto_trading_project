from _utils import read_config, validate_exchange, get_base_url, get_candlestick_url
import requests
import pandas as pd
from datetime import datetime, timedelta


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
    A dataframe of tickers for the given exchange

    Examples
    --------
    >>> get_ticker_ids('coinbase')
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
        exchange: str
        ) -> None:

        exchange = validate_exchange(exchange)
        self.exchange = exchange
        

    def get_data(self,
        ticker_id: str,
        start_date: str,
        end_date: str,
        date_format: str = '%Y-%m-%d',
        start_time: str = '00:00:00',
        end_time: str = '23:59:59',
        time_format: str = '%H:%M:%S',
        time_interval: str = '1d') -> pd.DataFrame:
        '''
        A method that returns a pandas dataframe with candlestick data from the exchange.

        Columns include time, low, high, open, close, and volume.
        Volume is expressed as the number of coins traded.

        Parameters
        ----------
        ticker_id: str
            The ticker id for which to pull candlestick data for.
        start_date: str
            The start date for the trading data provided as 'YYYY-MM-DD'. Different
            date formats can be provided by changing the date_format parameter.
        end_date: str
            The end date (inclusive) for the trading data provided as 'YYYY-MM-DD'.
            Different date formats can be provided by changing the date_format parameter.
        date_format: str
            The date format code of the start/end date provided. The available format codes 
            can be referenced in the datetime module documentation.
        start_time: str
            The start time for the trading data provided as 'HH:MM:SS'.
            The default is '00:00:00'. Like start_date, different
            time formats can be provided by changing the time_format parameter.
        end_time: str
            The end time for the trading data provided as 'HH:MM:SS'.
            The default is '23:59:59'. If using the default and end date as today,
            the record with the most recent candlestick data will be equal to the
            current candle. Like end_date, different time formats can be provided 
            by changing the time_format parameter.
        time_interval: str
            The time interval for the trading data. Valid arguments include:
            '1m', '5m', '15m', '1h', '6h', '1d'. Default is 1d.

        Returns
        -------
        A pandas dataframe with candlestick data.

        Examples
        --------
        >>> trades = Trades('coinbase', 'BTC-USD')
        >>> trades.get_data('2022-10-01', '2022-10-03')
            time	    low	        high	    open	    close	    volume
        0	2022-10-01	19160.00	19486.43	19423.57	19315.27	7337.455956
        1	2022-10-02	18923.81	19398.94	19315.26	19059.17	12951.424045
        2	2022-10-03	18958.29	19717.67	19059.10	19633.46	28571.640187
        '''

        #TODO need to provide support for indicators. It will ideally get added to this function since we want to pull the data once
        if not isinstance(ticker_id, str):
            raise TypeError('ticker_id must be a string')

        ticker_id = ticker_id.upper()

        valid_ids = get_all_tickers(self.exchange)['id'].values
        if ticker_id not in valid_ids:
            raise ValueError('Please provide a valid ticker id. Run get_all_tickers() for a full list of tickers')

        self.ticker_id = ticker_id

        #TODO need to see if I can update this so that the format is predicted versus having to explicitly state format.
        start_date = datetime.strptime(start_date, date_format).strftime('%Y-%m-%d')
        start_time = datetime.strptime(start_time, time_format).strftime('%H:%M:%S')
        start_datetime = datetime.strptime(start_date + ' ' + start_time, '%Y-%m-%d %H:%M:%S')
        #TODO need tell datetime object that times are in utc, then need to convert to users local time
        end_date = datetime.strptime(end_date, date_format).strftime('%Y-%m-%d')
        end_time = datetime.strptime(end_time, time_format).strftime('%H:%M:%S')
        end_datetime = datetime.strptime(end_date + ' ' + end_time, '%Y-%m-%d %H:%M:%S')

        time_interval_to_seconds = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '1h': 3600,
            '6h': 21600,
            '1d': 86400
        }

        #TODO will need to run time delta calcs to see if num records will be greater than 300, if so, then need to run loop.

        if time_interval not in time_interval_to_seconds.keys():
            msg = (
                "Please provide a valid time interval. "
                "Valid intervals are: "
            )
            raise KeyError(msg + str(list(time_interval_to_seconds.keys())))

        url = get_candlestick_url(self.exchange)
        url = url.format(ticker_id=self.ticker_id)
        
        payload = {
            'granularity': time_interval_to_seconds[time_interval],
            'start': start_datetime.strftime('%Y-%m-%d %H:%M:%S') ,
            'end': end_datetime.strftime('%Y-%m-%d %H:%M:%S')
            }

        response = requests.get(url, params=payload)

        if response.status_code != 200:
            raise Exception(f'Failed to retrieve data from {self.exchange} api')

        df = pd.DataFrame(data=response.json(), columns=['time', 'low', 'high', 'open', 'close', 'volume'])

        # converting unix time to datetime
        df['time'] = pd.to_datetime(df['time'], unit='s')

        # sorting by time
        df.sort_values(by=['time'], inplace=True, ignore_index=True)

        return df


        
