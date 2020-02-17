import datetime
import pandas as pd

from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.data_models.OrderFillEventDataModel import OrderFillEventDataModel
from src.data_models.SmartsDataModel import SmartsDataModel
from src.alert_analysis.SpoofingAnalysis import get_place_time_of_order


def account_split_matrix(account_id, instrument, evaluation_date, begin_date):

    if isinstance(instrument, str):
        instrument = [instrument]

    pd_data_frame_dict = {}
    for pair in instrument:
        data_set = SmartsDataModel().initialize(
            start_date=begin_date,
            evaluation_date=evaluation_date,
            other_condition="symbol = '{}' and account_id = {}".format(pair, account_id)
        ).evaluate()
        data_set['date_time'] = pd.to_datetime(data_set['event_date'].astype('str') + ' ' + data_set['event_time'].astype(str)) + data_set['event_millis']

        sub_data_set = get_place_time_of_order(data_set)
        available_order_ids = sub_data_set['order_id'].value_counts().index.tolist()

        # get maker/taker information
        data_from_order_fill_event = OrderFillEventDataModel().initialize(
            other_condition='order_id in {}'.format(available_order_ids).replace('[', '(').replace(']', ')')
        ).evaluate()[['order_id', 'liquidity_indicator']]

        sub_data_set = sub_data_set.merge(data_from_order_fill_event, how='left', on='order_id')
        sub_data_set['place_cancel_time_diff'] = sub_data_set['date_time'] - sub_data_set['place_time']
        sub_data_set['place_cancel_time_diff'] = sub_data_set['place_cancel_time_diff'].dt.total_seconds()

        # order without fill is place/cancel event
        sub_data_set['liquidity_indicator'] = sub_data_set['liquidity_indicator'].fillna('place_cancel')

        # account_split_matrix
        account_matrix = sub_data_set.groupby(['liquidity_indicator', 'side'])['place_cancel_time_diff'].value_counts()
        account_matrix = account_matrix.rename('timediff_counts').reset_index()

        pd_data_frame_dict.update({pair: account_matrix})

    return pd_data_frame_dict


if __name__ == '__main__':
    instruments = CryptoPairsDataModel().evaluate()['trading_pair'].tolist()
    data = account_split_matrix(202474, instruments, datetime.date(2019, 2, 28), datetime.date(2019, 1, 1))
