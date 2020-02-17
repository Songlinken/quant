import datetime
import pandas as pd

from src.calculation.LastTradeAndTradeBackDetail import LastTradeAndTradeBackDetail
from src.utility.GeneralUtility import timer
from src.utility.DataModelUtility import execute_query_data_frame


@timer
def test_last_trade_and_trade_back_detail(instrument, evaluation_date, max_trade_time_range, min_value_single_trade):
    """
    Write one day per instrument per time.

    :param instrument: str
    :param evaluation_date: date
    :param max_trade_time_range: datetime
    :param min_value_single_trade: numeric
    """
    calculation_model = LastTradeAndTradeBackDetail().initialize()

    data_set = calculation_model.get_data(instrument, evaluation_date, evaluation_date)[instrument]
    calculation_result = calculation_model.calculation(data_set, instrument, max_trade_time_range, min_value_single_trade)
    alerts = calculation_result.loc[~calculation_result['trade_time_range'].isna()]

    return alerts


if __name__ == '__main__':
    query = 'select instrument from available_instruments;'
    instruments = execute_query_data_frame(query, 'mktsrv', ssh=None, port=5432)['instrument'].tolist()
    evaluation_date = datetime.date(2019, 3, 19)

    alerts_dict = {}
    for pair in instruments:
        alert = test_last_trade_and_trade_back_detail(pair, evaluation_date, datetime.timedelta(minutes=15), 5000)
        alerts_dict.update({pair: alert})

    alerts_data_set = pd.concat(alerts_dict).reset_index(drop=True)
