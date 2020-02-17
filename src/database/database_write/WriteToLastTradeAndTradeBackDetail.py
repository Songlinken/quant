import datetime

from src.calculation.LastTradeAndTradeBackDetail import LastTradeAndTradeBackDetail
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.utility.GeneralUtility import timer
from src.utility.DataModelUtility import data_frame_to_sql


@timer
def write_to_last_trade_and_trade_back_detail(instrument, evaluation_date, max_trade_time_range, min_value_single_trade, stored_type, release_version):
    """
    Write one day per instrument per time.

    :param instrument: str
    :param evaluation_date: date
    :param max_trade_time_range: datetime
    :param min_value_single_trade: numeric
    :param stored_type: str
    :param version: int
    """
    calculation_model = LastTradeAndTradeBackDetail().initialize()

    data_set = calculation_model.get_data(instrument, evaluation_date, evaluation_date)
    calculation_result = calculation_model.calculation(data_set, instrument, max_trade_time_range, min_value_single_trade)
    calculation_result = calculation_result.assign(stored_type=stored_type, release_version=release_version)

    sub_calculation_result = calculation_result[['stored_type',
                                                 'release_version',
                                                 'account_id',
                                                 'event_id',
                                                 'event_date',
                                                 'event_type',
                                                 'date_time',
                                                 'symbol',
                                                 'side',
                                                 'last_traded_price',
                                                 'price_change_pct',
                                                 'time_from_last_trade',
                                                 'trade_value',
                                                 'counter_party_account_id',
                                                 'trade_back_time',
                                                 'trade_back_price',
                                                 'trade_time_range',
                                                 'money_pass']]

    data_frame_to_sql(sub_calculation_result, 'last_trade_and_trade_back_detail')


if __name__ == '__main__':
    instruments = CryptoPairsDataModel().evaluate()['trading_pair'].tolist()
    insert_date = datetime.datetime.today().date() - datetime.timedelta(days=1)

    for pair in instruments:
        write_to_last_trade_and_trade_back_detail(pair, insert_date, datetime.timedelta(minutes=15), 5000, 'c', '1')
