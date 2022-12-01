from _utils import read_config, validate_exchange, get_base_url
import requests
import pandas as pd


def get_valid_exchanges() -> list:
    valid_exchanges = read_config().keys()
    return list(valid_exchanges)


def get_ticker_ids(exchange: str) -> list:
    exchange = exchange.lower()

    if not validate_exchange(exchange):
        raise ValueError('Please provide a valid crypto exchange')

    url = get_base_url(exchange)
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    tickers = [ticker['id'] for ticker in response.json()]
    return sorted(tickers)


class Trades:

    def __init__(self,
        exchange: str,
        ticker_id: str) -> None:

        exchange = exchange.lower()
        ticker_id = ticker_id.upper()

        if not validate_exchange(exchange):
            raise ValueError('Please provide a valid crypto exchange. Run get_valid_exchanges() for a full list of exchanges')

        if ticker_id not in get_ticker_ids(exchange):
            raise ValueError('Please provide a valid ticker id. Run get_ticker_ids() for a full list of tickers')

        self.exchange = exchange
        self.ticker = ticker_id

    def get_data(self,
        start_date: str,
        end_date: str,
        start_time: str = '00:00:00',
        end_time: str = '23:59:59',
        time_interval: str = '1_day') -> pd.DataFrame:

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

        base_url = get_base_url(self.exchange)
        data_url = base_url + self.ticker + '/candles'

        headers = {"accept": "application/json"}
        
        payload = {
            'granularity': time_intervals[time_interval],
            'start': start_timestamp,
            'end': end_timestamp
            }

        response = requests.get(data_url, headers=headers, params=payload)

        df = pd.DataFrame(data=response.json(), columns=['time', 'low', 'high', 'open', 'close', 'volume'])

        # converting unix time to datetime
        df['time'] = pd.to_datetime(df['time'], unit='s')

        # sorting by time
        df.sort_values(by=['time'], inplace=True, ignore_index=True)

        return df


        
