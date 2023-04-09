
import pandas as pd

from datetime import datetime
import pytz
import time

from _utils import (
    read_config,
    validate_exchange,
    get_base_url,
    get_candlestick_url,
    get_row_count,
    get_date_ranges,
    get_requests,
    get_time_intervals
)


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
    A pandas dataframe containing trading pairs for the given exchange

    Examples
    --------
    >>> get_trading_pairs('coinbase')
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


class Candles:
    '''
    A class for pulling cryptocurrency candlestick trading data from a given exchange.

    Parameters
    ----------
    exchange : str
        The name of the crypto exchange to get data for.
    ticker_id : str
        The ticker id for which to pull candlestick data for.
    start_date : str
        The start date for the trading data provided as 'YYYY-MM-DD'. Different date formats
        can be provided by changing the date_format parameter listed below.
    end_date : str
        The end date (inclusive) for the trading data provided as 'YYYY-MM-DD'.
        Different date formats can be provided by changing the date_format parameter.
    date_format : str, optional
        The date format code for the start/end date provided. The default is '%Y-%m-%d'. The 
        available format codes can be referenced in the datetime module documentation.
    start_time : str, optional
        The start time for the trading data provided as 'HH:MM:SS'. The default is '00:00:00'.
        Like dates, different time formats can be provided by changing the time_format parameter.
    end_time : str, optional
        The end time for the trading data provided as 'HH:MM:SS'. The default is '23:59:59'.
        Like dates, different time formats can be provided by changing the time_format parameter.
    time_format : str, optional
        The time format code for the start/end time provided. The default is '%H:%M:%S'. The 
        available format codes can be referenced in the datetime module documentation.
    local_timezone : str, optional
        The local timezone. This will be needed when working with any time interval less
        than 1d. The default is America/New_York. See the pytz documentation for available timezones.
    time_interval : str, optional
        The time interval for the trading data. Valid arguments include:
        '1m', '5m', '15m', '1h', '6h', '1d'. The default is 1d.

    Returns
    -------
    **Attributes**
    df : pd.DataFrame
        A pandas dataframe with candlestick data. Columns include date, start_time, open, high, low,
        close, and volume. Volume is expressed as the number of coins traded. 

    Examples
    --------
    >>> btc_daily = Candles(exchange='coinbase',
                            ticker_id='BTC-USD',
                            start_date='2023-01-01',
                            end_date='2023-01-03')
    >>> btc_daily.df
        date	    open	    high	    low	        close	    volume
    0	2023-01-01	16531.83	16621.00	16490.00	16611.58	10668.736977
    1	2023-01-02	16611.90	16789.99	16542.52	16666.95	13560.460180
    2	2023-01-03	16666.86	16772.30	16600.00	16669.47	17612.355277

    >>> btc_hourly = Candles(exchange='coinbase',
                            ticker_id='BTC-USD',
                            start_date='2023-01-01',
                            end_date='2023-01-01',
                            start_time='12:00:00',
                            end_time='14:00:00',
                            time_interval='1h')
    >>> btc_hourly.df
        date	    start_time  open	    high	    low	        close	    volume
    0	2023-01-01	12:00:00	16556.33	16588.88	16554.60	16571.10	454.654269
    1	2023-01-01	13:00:00	16571.10	16595.62	16571.09	16583.74	313.306362
    2	2023-01-01	14:00:00	16583.70	16615.89	16583.69	16596.48	461.018155
    '''
  
    def __init__(
        self,
        exchange: str,
        ticker_id: str,
        start_date: str,
        end_date: str,
        date_format: str = '%Y-%m-%d',
        start_time: str = '00:00:00',
        end_time: str = '23:59:59',
        time_format: str = '%H:%M:%S',
        local_timezone: str = 'America/New_York',
        time_interval: str = '1d'
    )-> None:
        
        # validating the exchange and assigning it to self.exchange - will raise an error if exchange not found
        self.exchange = validate_exchange(exchange)

        # making sure ticker_id is a string
        if not isinstance(ticker_id, str):
            raise TypeError('ticker_id must be a string')

        # now uppercasing ticker_id just in case lowercase is provided
        ticker_id = ticker_id.upper()

        # gets the valid ticker_ids from the exchange and returns it as a numpy array
        valid_ids = get_trading_pairs(self.exchange)['id'].values
        
        # making sure ticker_id is found in array of tickers
        if ticker_id not in valid_ids:
            raise ValueError('Please provide a valid ticker id. Run get_trading_pairs() for a full list of tickers')

        self.ticker_id = ticker_id

        # getting the supported time intervals for the exchange from the config.yaml file
        # this will return a dictionary where the key is the time interval as shown above in the parameters
        # and the value will be the time interval in seconds, which is the value needed when requesting data from the api 
        time_intervals = get_time_intervals(self.exchange)

        # making sure the user provided a supported time interval
        if time_interval not in time_intervals.keys():
            msg = (
                "Please provide a valid time interval. "
                "Valid intervals are: "
            )
            raise KeyError(msg + str(list(time_intervals.keys())))
        
        self.time_interval = time_interval

        # getting the time in seconds for the provided time interval
        time_interval_in_seconds = time_intervals[self.time_interval]

        # now parsing the dates
        self.start_date = datetime.strptime(start_date, date_format)
        self.end_date = datetime.strptime(end_date, date_format)

        # now parsing the times
        self.start_time = datetime.strptime(start_time, time_format)
        self.end_time = datetime.strptime(end_time, time_format)

        # if the user selected the 1d time interval, let's hardcode the times to reduce chances of error
        if self.time_interval == '1d':
            self.start_time = datetime.strptime('00:00:00', '%H:%M:%S')
            self.end_time = datetime.strptime('23:59:59', '%H:%M:%S')

        # now combining the dates and times into datetimes
        self.start_datetime = datetime.combine(self.start_date.date(), self.start_time.time())
        self.end_datetime = datetime.combine(self.end_date.date(), self.end_time.time())

        # for all intervals less than 1d, we need to convert the users local time into utc time
        if self.time_interval != '1d':

            # get the local timezone from user
            local_timezone_object = pytz.timezone(local_timezone)

            # convert the start and end datetimes to utc
            self.start_datetime = local_timezone_object.localize(self.start_datetime).astimezone(pytz.utc)
            self.end_datetime = local_timezone_object.localize(self.end_datetime).astimezone(pytz.utc)
            
        # the coinbase api has a limit of 300 rows per request. In order to create a workaround
        # so that the user can pull more than 300 rows, we'll need to break the requests up into
        # smaller chunks. However, I do want to put an upper limit to how many rows the user is
        # requesting. To implement this, I'll need to calculate how many rows the user is requesting,
        # which is what this function does.
        number_of_rows = get_row_count(
            start_datetime=self.start_datetime,
            end_datetime=self.end_datetime,
            time_interval=self.time_interval,
            time_interval_in_seconds=time_interval_in_seconds
        )

        # gonna put a max 5K row count throttle on this for now
        if number_of_rows > 5000:
            msg = (
                "The number of rows you are attempting to retrieve is more than 5000,\n"
                "which is more than the api can handle. Try shortening your timeframe."
            )
            raise Exception(msg)
        
        # now that's out of the way, a list of date ranges can be made, which is what
        # the below function does
        date_ranges = get_date_ranges(
            start_datetime=self.start_datetime,
            final_end_datetime=self.end_datetime,
            time_interval_in_seconds=time_interval_in_seconds
        )
        
        # getting the candlestick url from the config.yaml file and formatting the string
        # with the given ticker id
        url = get_candlestick_url(self.exchange)
        url = url.format(ticker_id=self.ticker_id)
        
        # now going to loop through date ranges and append dataframe to a list
        list_of_dataframes = []
        for range in date_ranges:

            payload = {
                'granularity': time_interval_in_seconds,
                'start': range[0].strftime('%Y-%m-%d %H:%M:%S'),
                'end': range[1].strftime('%Y-%m-%d %H:%M:%S')
            }

            # these will be the returned colums
            columns=['datetime', 'low', 'high', 'open', 'close', 'volume']

            # sending the request to the api
            df = get_requests(
                url=url,
                payload=payload,
                columns=columns
            )

            list_of_dataframes.append(df)

            time.sleep(1)

        df = pd.concat(list_of_dataframes, ignore_index=True)

        # converting unix time to datetime
        df['datetime'] = pd.to_datetime(df['datetime'], unit='s', utc=True)

        # converting utc time back to user's local time if time interval is not equal to 1d
        if self.time_interval != '1d':
            df['datetime'] = df['datetime'].dt.tz_convert(pytz.timezone(local_timezone))

        # sorting by time
        df.sort_values(by=['datetime'], inplace=True, ignore_index=True)

        # creating a date and time column from datetime for readability
        df['date'] = df['datetime'].dt.date
        df['start_time'] = df['datetime'].dt.time

        # rearranging the columns and dropping datetime
        df = df[['date', 'start_time', 'open', 'high', 'low', 'close', 'volume']]

        # dropping the start_time column if its the daily time interval since they all show 00:00:00
        if self.time_interval == '1d':
            df = df.drop('start_time', axis=1)

        self.df = df


#TODO improve get_date_ranges function
#TODO set up doctests
#TODO add indicator methods
#TODO figure out how to make docstring dataframe column headers centered
