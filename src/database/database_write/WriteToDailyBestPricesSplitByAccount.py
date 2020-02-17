import datetime

from src.calculation.DailyBestPrices import DailyBestPrices
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.utility.DataModelUtility import data_frame_to_sql
from src.utility.GeneralUtility import timer


@timer
def write_to_daily_best_prices_split_by_account(instrument, evaluation_date):
    """
    Write data for one day per instrument per time. Only cache best three prices.

    :param instrument: str
    :param evaluation_date: date
    :param use_db: bool
    """
    calculation_module = DailyBestPrices()

    data_set = calculation_module.get_data(instrument, evaluation_date, evaluation_date)
    calculation_result = calculation_module.calculation(data_set, instrument, 10, split_best_volume_to_account=True)

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
                                                 'best_price_4',
                                                 'best_price_5',
                                                 'best_price_6',
                                                 'best_price_7',
                                                 'best_price_8',
                                                 'best_price_9',
                                                 'best_price_10',
                                                 'best_account_volume_1',
                                                 'best_account_volume_2',
                                                 'best_account_volume_3',
                                                 'best_account_volume_4',
                                                 'best_account_volume_5',
                                                 'best_account_volume_6',
                                                 'best_account_volume_7',
                                                 'best_account_volume_8',
                                                 'best_account_volume_9',
                                                 'best_account_volume_10',
                                                 'best_price_1_other_side',
                                                 'best_price_2_other_side',
                                                 'best_price_3_other_side',
                                                 'best_price_4_other_side',
                                                 'best_price_5_other_side',
                                                 'best_price_6_other_side',
                                                 'best_price_7_other_side',
                                                 'best_price_8_other_side',
                                                 'best_price_9_other_side',
                                                 'best_price_10_other_side',
                                                 'best_account_volume_1_other_side',
                                                 'best_account_volume_2_other_side',
                                                 'best_account_volume_3_other_side',
                                                 'best_account_volume_4_other_side',
                                                 'best_account_volume_5_other_side',
                                                 'best_account_volume_6_other_side',
                                                 'best_account_volume_7_other_side',
                                                 'best_account_volume_8_other_side',
                                                 'best_account_volume_9_other_side',
                                                 'best_account_volume_10_other_side']]

    sub_calculation_result = sub_calculation_result.loc[sub_calculation_result['event_type'] != 'Initial']

    data_frame_to_sql(sub_calculation_result, 'daily_best_prices_split_by_account')


if __name__ == '__main__':
    instruments = CryptoPairsDataModel().evaluate()['trading_pair'].tolist()
    current_instruments = ['BCHBTC', 'BCHETH', 'BCHUSD',
                           'BTCUSD',
                           'ETHBTC', 'ETHUSD',
                           'LTCBCH', 'LTCBTC', 'LTCETH', 'LTCUSD',
                           'ZECBCH', 'ZECBTC', 'ZECETH', 'ZECLTC', 'ZECUSD']

    if not all(pair in instruments for pair in current_instruments):
        raise ValueError('current_instruments are not in instruments.')

    insert_date = datetime.datetime.today().date() - datetime.timedelta(days=1)

    for instrument in current_instruments:
        write_to_daily_best_prices_split_by_account(instrument, insert_date)
