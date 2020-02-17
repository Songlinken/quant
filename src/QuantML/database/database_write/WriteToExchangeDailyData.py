import datetime

from src.QuantML.data_source.CryptoCompare import CryptoCompare
from src.utility.DataModelUtility import data_frame_to_sql
from src.utility.GeneralUtility import timer


@timer
def write_to_exchange_daily_data(instrument_from,
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

    calculation_module = CryptoCompare()

    data_set = calculation_module.get_historical_data_to_dataframe(instrument_from=instrument_from,
                                                                   instrument_to=instrument_to,
                                                                   exchange=exchange,
                                                                   interval=interval,
                                                                   all_data=all_data,
                                                                   input_date=input_date)

    data_set = data_set.rename({'open': 'open_price',
                                'close': 'close_price',
                                'high': 'high_price',
                                'low': 'low_price',
                                'volumefrom': 'volume_from',
                                'volumeto': 'volume_to',
                                'time_stick': 'event_date'}, axis=1)

    result_date_set = data_set[['event_date', 'symbol', 'open_price', 'close_price', 'high_price', 'low_price', 'volume_from', 'volume_to']]

    data_frame_to_sql(result_date_set, '{}_daily_data'.format(exchange).lower(), schema='public', ssh=None, database='mktsrv', port=5432)


if __name__ == '__main__':
    for exchange in ['Gemini', 'CCCAGG']:
        write_to_exchange_daily_data('BTC', 'USD', exchange=exchange, all_data=False)
