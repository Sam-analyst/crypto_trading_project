from _utils import read_config, validate_exchange, get_base_url, get_candlestick_url, get_datetime, get_row_count, create_date_ranges, get_requests
import requests
import pandas as pd
import pytz
import time


def get_valid_exchanges() -> list:
    '''
    A function that returns a list of crypto exchanges that are supported by this code.
    In other words, you can only pull candlestick data for the exchanges that are in this list.

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

    # read_config() reads the config.yaml file and returns it as a dictionary
    valid_exchanges = read_config().keys() # getting the keys since these are the exchanges

    return list(valid_exchanges)


def get_trading_pairs(exchange: str) -> pd.DataFrame:
    '''
    A function that returns a dataframe containing all trading pairs from the exchange.
    One thing to be aware is that not all trading pairs returned from the exchange
    are active. Review the 'status' column to determine which trading pairs are active.

    Parameters
    ----------
    exchange : str
        The name of the crypto exchange to get trading pairs for

    Returns
    -------
    A dataframe of trading pairs for the given exchange

    Examples
    --------
    >>> get_ticker_ids('coinbase')
    '''

    # validating the exchange - will raise an error if exchange not found
    exchange = validate_exchange(exchange)

    # gets the base url from the exchange
    url = get_base_url(exchange)

    # sends the request to the url
    df = get_requests(url=url)

    # sorting by id
    df = df.sort_values('id', ignore_index=True)

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
        local_timezone: str = 'America/New_York',
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
        local_timezone: str
            The local timezone. This will be needed when working with any time interval less
            than 1d. See the pytz documentation for available timezones.
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

        if not isinstance(ticker_id, str):
            raise TypeError('ticker_id must be a string')

        ticker_id = ticker_id.upper()

        valid_ids = get_trading_pairs(self.exchange)['id'].values
        
        if ticker_id not in valid_ids:
            raise ValueError('Please provide a valid ticker id. Run get_all_tickers() for a full list of tickers')

        self.ticker_id = ticker_id

        time_interval_to_seconds = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '1h': 3600,
            '6h': 21600,
            '1d': 86400
        }

        if time_interval not in time_interval_to_seconds.keys():
            msg = (
                "Please provide a valid time interval. "
                "Valid intervals are: "
            )
            raise KeyError(msg + str(list(time_interval_to_seconds.keys())))
        
        # assigning a variable for time interval in seconds
        time_interval_in_seconds = time_interval_to_seconds[time_interval]

        if time_interval == '1d':

            start_datetime = get_datetime(date=start_date,
                                          time='00:00:00', # overriding users input for daily intervals so returned results are accurate
                                          date_format=date_format,
                                          time_format=time_format)
             
            end_datetime = get_datetime(date=end_date,
                                        time='23:59:59', # overriding users input for daily intervals so returned results are accurate
                                        date_format=date_format,
                                        time_format=time_format)
        
        else:
            
            start_datetime = get_datetime(date=start_date,
                                          time=start_time,
                                          date_format=date_format,
                                          time_format=time_format,
                                          convert_to_utc=True,
                                          local_timezone=local_timezone)
             
            end_datetime = get_datetime(date=end_date,
                                        time=end_time,
                                        date_format=date_format,
                                        time_format=time_format,
                                        convert_to_utc=True,
                                        local_timezone=local_timezone)
            

        number_of_rows = get_row_count(start_datetime=start_datetime,
                                       end_datetime=end_datetime,
                                       time_interval=time_interval,
                                       time_interval_in_seconds=time_interval_in_seconds
                                       )

        if number_of_rows > 5000:
            msg = (
                "The number of rows you are attempting to retrieve is more than 5000,\n"
                "which is more than the api can handle. Try shortening your timeframe."
                )
            raise Exception(msg)
        
        date_ranges = create_date_ranges(start_date=start_datetime,
                                         final_end_date=end_datetime,
                                         time_interval_in_seconds=time_interval_in_seconds
                                         )
        
        url = get_candlestick_url(self.exchange)
        url = url.format(ticker_id=self.ticker_id)
        
        list_of_dataframes = []
        for range in date_ranges:

            payload = {
                'granularity': time_interval_in_seconds,
                'start': range[0],
                'end': range[1]
                }

            response = requests.get(url, params=payload)

            if response.status_code != 200:
                raise Exception(f'Failed to retrieve data from {self.exchange} api')

            df = pd.DataFrame(data=response.json(), columns=['time', 'low', 'high', 'open', 'close', 'volume'])

            # converting unix time to datetime
            df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)

            if time_interval != '1d':
                # converting utc to local_timezone
                df['time'] = df['time'].dt.tz_convert(pytz.timezone(local_timezone))

            list_of_dataframes.append(df)

            time.sleep(1)

        df = pd.concat(list_of_dataframes, ignore_index=True)

        # sorting by time
        df.sort_values(by=['time'], inplace=True, ignore_index=True)

        return df

#TODO change _utils import to import everything and then just use _utils.function
#TODO need to add comments in code
#TODO identify areas to reduce redundency
#TODO add docstrings to every function
        
