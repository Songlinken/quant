import numpy as np
import os
import pandas as pd
import datetime
import warnings

from src.calculation.CalculationNode import CalculationNode
from src.data_models.SmartsDataModel import SmartsDataModel
from src.utility.GeneralUtility import timer

warnings.simplefilter(action='ignore', category=FutureWarning)


class LastTradeAndTradeBackDetail(CalculationNode):
    """
    This class designed to get trade back time & counter party account which will be used for SMARTS wash sale alerts.
    Calculation result could be cached into database last_trade_and_trade_back_detail table if necessary.
    """
    def initialize(self, **kwargs):
        """
        Initialize necessary class variables.
        """
        if not kwargs:
            self.arguments = {'home': os.path.expanduser('~'),
                              'class_name': self.__class__.__name__}

        self.arguments.update(kwargs)

        return self

    def get_data(self, instrument, evaluation_date, begin_date):
        """
        Pull data from interim database, otherwise download data from datalab.

        :param instrument: str (Example: 'BTCUSD')
        :param evaluation_date: date
        :param begin_date: date
        :return: pd dataframe
        """
        data_set = SmartsDataModel().initialize(evaluation_date=evaluation_date, begin_date=begin_date, other_condition="symbol = '{}'".format(instrument)).evaluate()
        data_set['date_time'] = pd.to_datetime(data_set['event_date'].astype('str') + ' ' + data_set['event_time'].astype(str)) + data_set['event_millis']

        return data_set

    @timer
    def calculation(self, data_set, instrument, max_trade_time_range, min_value_single_trade):
        """
        Get last trade and trade back detail of the given dataframe. Only works one day per instrument at a time.

        :param data_set: pd dataframe
        :param instrument: string
        :param max_trade_time_range: timedelta
        :param min_value_single_trade: numeric
        :param min_order_time_range: timedelta
        :param max_order_time_range: timedelta
        :return: pd dataframe
        """
        if not (data_set['symbol'] == instrument).all():
            raise ValueError('data_set provided does not match the instrument {}'.format(instrument))

        if (data_set.loc[data_set['event_type'] != 'Initial', 'date_time'].max() - data_set.loc[data_set['event_type'] != 'Initial', 'date_time'].min()
                > datetime.timedelta(days=1)):
            raise ValueError('data_set has value larger than 24 hours.')

        data_set['last_traded_price'] = data_set.loc[data_set['event_type'] == 'Fill', 'fill_price'].shift()
        data_set['price_change_pct'] = ((data_set['fill_price'] / data_set['last_traded_price']) - 1) * 100
        data_set['time_from_last_trade'] = data_set.loc[data_set['event_type'] == 'Fill', 'date_time'].diff()
        data_set['trade_value'] = data_set['fill_price'] * data_set['fill_quantity_crypto']
        data_set = self.get_counter_party(data_set)

        # auction trade does not have counter party
        sub_data_set = data_set.loc[(data_set['event_type'] == 'Fill') & (data_set['auction_id'].isna())]

        if sub_data_set.empty:
            return sub_data_set.assign(trade_back_time=pd.NaT, trade_back_price=np.nan, time_from_last_trade=pd.NaT, trade_time_range=pd.NaT, money_pass=np.nan)

        sub_data_set = (sub_data_set.groupby(['account_id', 'counter_party_account_id', 'fill_quantity_crypto'])
                        .apply(self.get_trade_back_time_and_price, max_trade_time_range, min_value_single_trade)
                        .reset_index(drop=True))

        sub_data_set['money_pass'] = sub_data_set['trade_back_price'] * sub_data_set['fill_quantity_crypto'] - sub_data_set['trade_value']
        sub_data_set['trade_time_range'] = sub_data_set['trade_back_time'] - sub_data_set['date_time']
        sub_data_set['time_from_last_trade'] = pd.to_datetime(sub_data_set['time_from_last_trade'].dt.total_seconds(), unit='s')
        sub_data_set['trade_time_range'] = pd.to_datetime(sub_data_set['trade_time_range'].dt.total_seconds(), unit='s')
        sub_data_set['time_from_last_trade'] = sub_data_set['time_from_last_trade'].dt.time
        sub_data_set['trade_time_range'] = sub_data_set['trade_time_range'].dt.time

        return sub_data_set

    @timer
    def get_counter_party(self, data_set):
        """
        Get counter party account id based on event id. Use column key for looping to speed up.
        """
        sub_data_set = data_set.loc[(data_set['event_type'] == 'Fill') & (data_set['auction_id'].isna())]
        sub_data_set = sub_data_set.assign(counter_party_account_id=np.nan)
        columns = sub_data_set.columns
        event_id = sub_data_set['event_id'].unique()

        # construct map
        key_pairs = dict()

        for id_ in event_id:
            key_pairs[id_] = []

        # map values
        data = sub_data_set.to_numpy()
        for row in range(data.shape[0]):
            # column 2 is event_id in data
            pair_key = data[row, 2]
            temp_data = key_pairs[pair_key]

            if not temp_data:
                # 1st account_id associated with the event_id, data column 0 is account_id
                temp_data += [row, data[row, 0]]

            else:
                # 2nd account_id associated with the event_id, temp_data column 2 stores 1st account id
                data[row, -1] = temp_data[1]

                # temp_data column 1 stores the row of 1st account_id
                data[temp_data[0], -1] = data[row, 0]

        sub_data_set = (pd.DataFrame(data, columns=columns)[['account_id', 'event_id', 'event_type', 'counter_party_account_id']]
                        .astype({'account_id': 'int64', 'event_id': 'int64', 'event_type': str, 'counter_party_account_id': 'int64'}))

        data_set = data_set.merge(sub_data_set, how='left', on=['account_id', 'event_id', 'event_type'])

        return data_set

    def get_trade_back_time_and_price(self, data_set, max_trade_time_range, min_value_single_trade):
        """
        Get trade back time for transaction within max_trade_time_range with same quantity.
        (Example: A -> B -> A with 2 BTC, trade_back_time for A is the time at B -> A.)

        :param data_set: pd dataframe
        :param max_trade_time_range: timedelta
        :param min_value_single_trade: numeric
        :return: pd dataframe
        """
        data_set = data_set.assign(trade_back_time=pd.NaT, trade_back_price=np.nan)
        sides = data_set['side'].unique()

        if len(sides) > 1:
            initial_side = sides[0]
            other_side = sides[1]
            index_initial_side = data_set.loc[(data_set['side'] == initial_side) & (data_set['trade_value'] >= min_value_single_trade)].index.tolist()
            index_other_side = data_set.loc[(data_set['side'] == other_side) & (data_set['trade_value'] >= min_value_single_trade)].index.tolist()

            for index_initial in index_initial_side:
                for index_other in index_other_side:
                    if data_set.loc[index_other, 'date_time'] - data_set.loc[index_initial, 'date_time'] > max_trade_time_range:
                        break

                    elif datetime.timedelta(0) <= data_set.loc[index_other, 'date_time'] - data_set.loc[index_initial, 'date_time'] <= max_trade_time_range:
                        data_set.loc[index_initial, 'trade_back_time'] = data_set.loc[index_other, 'date_time']
                        data_set.loc[index_initial, 'trade_back_price'] = data_set.loc[index_other, 'fill_price']
                        index_other_side.remove(index_other)
                        break

                    elif -max_trade_time_range <= data_set.loc[index_other, 'date_time'] - data_set.loc[index_initial, 'date_time'] < datetime.timedelta(0):
                        data_set.loc[index_other, 'trade_back_time'] = data_set.loc[index_initial, 'date_time']
                        data_set.loc[index_other, 'trade_back_price'] = data_set.loc[index_initial, 'fill_price']
                        index_other_side.remove(index_other)
                        break

        return data_set
