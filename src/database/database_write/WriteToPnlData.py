import datetime
import pandas as pd

from src.calculation.PnLByAccount import PnLByAccount
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.utility.DataModelUtility import data_frame_to_sql
from src.utility.GeneralUtility import timer


@timer
def write_to_daily_best_prices_split_by_account(instrument, evaluation_date, initialize_benchmark=False):
    """
    Write data for one day per instrument per time. Only cache best three prices.

    :param instrument: str
    :param evaluation_date: date
    :param initialize_benchmark: bool
    """
    calculation_result = PnLByAccount().evaluate(instrument, evaluation_date, initialize_benchmark)
    calculation_result = pd.concat(calculation_result)

    sub_calculation_result = calculation_result[['account_id',
                                                 'event_id',
                                                 'event_date',
                                                 'created',
                                                 'trading_pair',
                                                 'side',
                                                 'order_book',
                                                 'quantity',
                                                 'price',
                                                 'pnl',
                                                 'unpnl',
                                                 'queues']]

    data_frame_to_sql(sub_calculation_result, 'pnl_data')


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
    write_to_daily_best_prices_split_by_account(current_instruments, insert_date)
