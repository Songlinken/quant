import datetime

from src.calculation.MarketMakerIdentificationMatrix import MarketMakerIdentificationMatrix
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.utility.DataModelUtility import data_frame_to_sql
from src.utility.GeneralUtility import timer


@timer
def write_to_market_maker_identification_matrix(instrument, evaluation_date):
    """
    Write data for one day per instrument per time.

    :param instrument: str
    :param evaluation_date: date
    :param use_db: bool
    """
    calculation_module = MarketMakerIdentificationMatrix()
    calculation_result = calculation_module.evaluate(instrument, evaluation_date)
    calculation_result['market_maker'] = None

    sub_calculation_result = calculation_result[['account_id',
                                                 'data_from_date',
                                                 'symbol',
                                                 'place_cancel_time_diff',
                                                 'bid_ask_ratio_median',
                                                 'bid_ask_ratio_std',
                                                 'is_institutional',
                                                 'fill_buy_orders_count',
                                                 'fill_sell_orders_count',
                                                 'fill_buy_orders_quantity',
                                                 'fill_sell_orders_quantity',
                                                 'total_orders',
                                                 'cancel_orders',
                                                 'maker_counts',
                                                 'taker_counts',
                                                 'market_maker']]

    data_frame_to_sql(sub_calculation_result, 'market_maker_identification_matrix')


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
        write_to_market_maker_identification_matrix(instrument, insert_date)
