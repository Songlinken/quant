import datetime
import quandl

from src.utility.DataModelUtility import data_frame_to_sql
from src.utility.GeneralUtility import timer


@timer
def write_to_metal_daily_data(metal='gold',
                              unit='oz',
                              all_data=False,
                              input_date=datetime.date.today() - datetime.timedelta(days=4)):
    """
    Get historical data from Crypto Compare.
    :param metal: str
    :param unit: str
    :param all_data: bool
    :param input_date: date (if all_data=False, this is data of the date to get)
    :return: pd.Dataframe
    """
    if metal == 'gold':
        metal_str = 'WGC/{}_DAILY_USD'.format(metal.upper())

    else:
        raise ValueError('metal must be gold.')

    if all_data:
        data_set = quandl.get(metal_str,
                              authtoken='Jszm-d1BscjHYRVaMmUX')

    else:
        data_set = quandl.get(metal_str,
                              authtoken='Jszm-d1BscjHYRVaMmUX',
                              start_date=input_date,
                              end_date=input_date)

    data_set = data_set.reset_index().rename({'Date': 'event_date',
                                              'Value': 'close_price'}, axis=1)

    data_set['metal'] = metal
    data_set['unit'] = unit
    data_set = data_set[['event_date', 'metal', 'unit', 'close_price']]

    data_frame_to_sql(data_set, 'metal_daily_data', schema='public', ssh=None, database='mktsrv', port=5432)


if __name__ == '__main__':
    write_to_metal_daily_data(all_data=True)
