import datetime

from src.calculation.DailyBestPrices import DailyBestPrices
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.utility.DataModelUtility import data_frame_to_sql
from src.utility.GeneralUtility import timer


@timer
def write_to_daily_best_prices(instrument, evaluation_date):
    """
    Write data for one day per instrument per time. Only cache best three prices.

    :param instrument: str
    :param evaluation_date: date
    :param use_db: bool
    """
    calculation_module = DailyBestPrices()

    data_set = calculation_module.get_data(instrument, evaluation_date, evaluation_date)
    calculation_result = calculation_module.calculation(data_set, instrument, 3)

    sub_calculation_result = calculation_result[['account_id',
                                                 'event_id',
                                                 'event_date',
                                                 'event_type',
                                                 'date_time',
                                                 'symbol',
                                                 'side',
                                                 'best_price_1',
                                                 'best_price_2',
                                                 'best_price_3',
                                                 'best_volume_1',
                                                 'best_volume_2',
                                                 'best_volume_3']]

    sub_calculation_result = sub_calculation_result.loc[sub_calculation_result['event_type'] != 'Initial']

    data_frame_to_sql(sub_calculation_result, 'daily_best_prices')


if __name__ == '__main__':
    instruments = CryptoPairsDataModel().evaluate()['trading_pair'].tolist()
    insert_date = datetime.datetime.today().date() - datetime.timedelta(days=1)
    for instrument in instruments:
        write_to_daily_best_prices(instrument, insert_date)
