import datetime
import pandas as pd
import requests


class CryptoCompare(object):
    """
    Crypto Compare client API.
    """
    def __init__(self,
                 api_url: str = 'https://min-api.cryptocompare.com/data/',
                 request_timeout: int = 30):

        # creates a crypto compare API client instance.
        self.url = api_url.rstrip('/')
        self.auth = None
        self.session = requests.Session()
        self.request_timeout = request_timeout

    def send_message(self,
                     method: str,
                     endpoint: str,
                     params: dict = None,
                     data: str = None):

        # send API request.
        url = self.url + endpoint
        response = self.session.request(method,
                                        url,
                                        params=params,
                                        data=data,
                                        auth=self.auth,
                                        timeout=self.request_timeout)

        return response.json()

    def get_historical_data_to_dataframe(self,
                                         instrument_from,
                                         instrument_to,
                                         exchange='Gemini',
                                         interval='day',
                                         all_data=False,
                                         input_date=datetime.date.today() - datetime.timedelta(days=1)):
        """
        Get historical data from Crypto Compare.
        :param instrument_from: str
        :param instrument_to: str
        :param exchange: str
        :param interval: str (day, hour, minute)
        :param all_data: bool
        :param input_date: date (if all_data=False, this is data of the date to get)
        :return: pd.Dataframe
        """
        if interval not in ['day', 'hour', 'minute']:
            raise ValueError('interval must be day, hour or minute.')

        if all_data:
            all_data_str = 'true'

        else:
            all_data_str = 'false'

        respond = self.send_message(method='get',
                                    endpoint='/histo{}?fsym={}&tsym={}&e={}&allData={}'.format(interval, instrument_from, instrument_to, exchange, all_data_str))

        data_set = pd.DataFrame(respond['Data'])
        data_set['time_stick'] = pd.to_datetime(data_set['time'], unit='s')
        data_set = data_set.drop('time', axis=1)
        data_set['symbol'] = instrument_from + instrument_to

        if not all_data:
            data_set = data_set.loc[data_set['time_stick'].dt.date == input_date]

        return data_set


if __name__ == '__main__':
    data = CryptoCompare().get_historical_data_to_dataframe('BTC', 'USD', all_data=False)
