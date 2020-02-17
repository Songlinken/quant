import numpy as np
import os
import pandas as pd
import datetime
import warnings

from src.calculation.CalculationNode import CalculationNode
from src.data_models.SmartsCsvDataModel import SmartsCsvDataModel
from src.utility.GeneralUtility import timer

warnings.simplefilter(action='ignore', category=FutureWarning)


class BaitAndSwitch(CalculationNode):
    """
    This class is designed to trigger same alters as SMARTS does for alert 4022.
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

    def get_data(self, instrument, evaluation_date, begin_date, download_data=True, unzip=True, force=False):
        """
        Pull data from local machine, otherwise download data from datalab.

        :param instrument: str or list (Example: 'BTCUSD' or ['BTCUSD', 'LTCUSD'])
        :param evaluation_date: date
        :param begin_date: date
        :param download_data: bool
        :param unzip: bool
        :param force: bool (override downloaded csv if existing)
        :return: dict with pd dataframe
        """
        data_set_dict = SmartsCsvDataModel(download_data).evaluate(instrument, evaluation_date, begin_date, unzip, force)

        return data_set_dict

    @timer
    def calculation(self, data_set, instrument, evaluation_date, begin_date, num_of_best_price):
        """
        Calculate all necessary columns for alert_analysis, such as best price and cpv.

        :param data_set: pd dataframe
        :param instrument: str
        :param evaluation_date: date
        :param begin_date: date
        :param num_of_best_price: int
        :param cache_result: bool
        :return: pd dataframe
        """
        cached_data_set = self.get_cache_result(instrument, num_of_best_price)

        if not cached_data_set.empty:
            cached_date = cached_data_set['event_date'].value_counts().index.date
            data_set = data_set.loc[~data_set['event_date'].isin(cached_date)]

        if data_set.empty:
            calculation_result = cached_data_set

        else:
            # calculate cpv & best price
            calculation_result = data_set.groupby(['event_date', 'side']).apply(self.get_best_price_and_cpv_of_order, num_of_best_price)

            # calculate total trade volume within the time of each order
            calculation_result['start_time_of_order'] = calculation_result.groupby('order_id')['date_time'].transform('min')
            calculation_result['latest_time_of_order'] = calculation_result.groupby('order_id')['date_time'].transform('max')
            calculation_result = calculation_result.groupby('account_id').apply(self.get_trade_volume_and_delay_time)

            # concatenate new data to cached data
            if not cached_data_set.empty:
                calculation_result = pd.concat([cached_data_set, calculation_result]).sort_values('date_time')

            if cache_result:
                self.cache_result(calculation_result, instrument, num_of_best_price)

        date_filter = (calculation_result['event_date'] >= begin_date) & (calculation_result['event_date'] <= evaluation_date)

        return calculation_result[date_filter]

    @timer
    def trigger_alerts(self, data_set, days_of_benchmark=0, volume_threshold_multiplier=10):
        # use first 100 trade if not enough days to calculate threshold
        volume_threshold = (data_set.loc[(data_set['event_type'] == 'Fill') & (data_set['side'] == 'sell'), 'fill_quantity']
                            .head(100).mean() * volume_threshold_multiplier)
        import ipdb;ipdb.set_trace()
        execute_orders = data_set.loc[data_set['event_type'] == 'Fill', 'order_id']
        data_set_non_execute = data_set.loc[~data_set['order_id'].isin(execute_orders)]

    @timer
    def get_best_price_and_cpv_of_order(self, data_set, num_of_best_price):
        """
        Attention: date_time column must be sorted before apply this function.
        """
        for index_ in data_set.index:
            sub_data_set = data_set.loc[:index_]

            # check side of the order (buy or sell)
            side = sub_data_set['side'].unique()[0]

            # alive orders on the order book
            mask = (sub_data_set['event_type'] == 'Cancel') | ((sub_data_set['event_type'] == 'Fill') & (sub_data_set['remaining_quantity'] == 0))
            filled_or_cancelled_order = sub_data_set.loc[mask, 'order_id']
            alive_order = sub_data_set.loc[~sub_data_set['order_id'].isin(filled_or_cancelled_order)]

            # keep only partially filled order if order is not filled completely
            sub_orders = alive_order.groupby('order_id')['remaining_quantity'].idxmin()
            sub_data_set = sub_data_set.loc[sub_data_set['order_id'].index.isin(sub_orders)]

            # calculate total volume for each of the available price
            sub_data_set['total_volume_of_same_limit_price'] = sub_data_set.groupby('limit_price')['remaining_quantity'].transform('sum')

            all_prices = sub_data_set['limit_price'].values.tolist()
            all_prices = [price for price in all_prices if ~np.isnan(price)]
            all_prices = list(set(all_prices))

            # calculate cumulative priority volume (CPV)
            if side == 'buy':
                all_prices.sort()
                if len(all_prices) >= num_of_best_price and data_set.loc[index_, 'limit_price'] >= all_prices[-num_of_best_price]:
                    cpv = (sub_data_set
                           .loc[sub_data_set['limit_price'].isin(all_prices[-num_of_best_price:]), ['limit_price', 'total_volume_of_same_limit_price']]
                           .drop_duplicates()
                           .head(num_of_best_price)['total_volume_of_same_limit_price']
                           .sum())

                else:
                    cpv = np.nan
            else:
                all_prices.sort(reverse=True)
                if len(all_prices) >= num_of_best_price and data_set.loc[index_, 'limit_price'] <= all_prices[-num_of_best_price]:
                    cpv = (sub_data_set
                           .loc[sub_data_set['limit_price'].isin(all_prices[-num_of_best_price:]), ['limit_price', 'total_volume_of_same_limit_price']]
                           .drop_duplicates()
                           .head(num_of_best_price)['total_volume_of_same_limit_price']
                           .sum())

                else:
                    cpv = np.nan

            data_set.loc[index_, 'best_price'] = all_prices[-1]
            data_set.loc[index_, 'cpv_of_{num_of_best_price}_best_price'.format(num_of_best_price=num_of_best_price)] = cpv

        return data_set

    def get_trade_volume_and_delay_time(self, data_set):
        """
        Get total trade volume within active time for each order, and get last trade time on the other side if order was cancelled.
        """
        for value in data_set.itertuples():
            total_trade_volume = data_set.loc[(data_set['date_time'] >= getattr(value, 'start_time_of_order'))
                                              & (data_set['date_time'] <= getattr(value, 'latest_time_of_order')), 'fill_quantity'].sum()

            data_set.loc[getattr(value, 'Index'), 'trade_volume_within_order_time'] = total_trade_volume

            if getattr(value, 'event_type') == 'Cancel':
                last_trade_time_on_other_side = data_set.loc[(data_set['side'] != getattr(value, 'side'))
                                                             & (data_set['event_type'] == 'Fill')
                                                             & (data_set['date_time'] < getattr(value, 'date_time')), 'date_time'].max()

                data_set.loc[getattr(value, 'Index'), 'last_trade_time_on_other_side'] = last_trade_time_on_other_side

        return data_set

    @timer
    def get_cache_result(self, instrument, num_of_best_price):
        local_path = (self.arguments['home'] + '/Documents/Calculation_result/{class_name}/{instrument}/{num_of_best_price}_of_best_price/'
                      .format(class_name=self.arguments['class_name'], instrument=instrument, num_of_best_price=num_of_best_price))

        if not os.path.exists(local_path):
            print('No cache result for {instrument}.'.format(instrument=instrument))
            return pd.DataFrame()

        else:
            cache_files = [file for file in os.listdir(local_path) if file.startswith(instrument)]

            if cache_files:
                # pick the most recent modified cache file
                cache_files.sort(key=lambda fn: os.path.getmtime(local_path + fn), reverse=True)
                most_recent_file = cache_files[0]
                calculation_result = pd.read_pickle(local_path + most_recent_file)

            else:
                print('No cache result for {instrument}.'.format(instrument=instrument))
                calculation_result = pd.DataFrame()

            return calculation_result

    @timer
    def cache_result(self, data_set, instrument, num_of_best_price):

        local_path = (self.arguments['home'] + '/Documents/Calculation_result/{class_name}/{instrument}/{num_of_best_price}_of_best_price/'
                      .format(class_name=self.arguments['class_name'], instrument=instrument, num_of_best_price=num_of_best_price))

        if not os.path.exists(local_path):
            os.makedirs(local_path)

        max_date = data_set['event_date'].max().date()
        min_date = data_set['event_date'].min().date()

        data_set.to_pickle(local_path + '{instrument}_{class_name}_{min_date}_to_{max_date}.pkl'
                           .format(instrument=instrument, class_name=self.arguments['class_name'], min_date=min_date, max_date=max_date))


if __name__ == '__main__':

    instruments = ['LTCUSD']
    num_best_price = 3
    evaluation_date = datetime.date(2018, 10, 16)
    beginning_date = datetime.date(2018, 10, 16)

    data_model = BaitAndSwitch().initialize()
    data_dict = data_model.get_data(instrument=instruments,
                                    evaluation_date=evaluation_date,
                                    begin_date=beginning_date)
    # clearning & calculation
    final_dict = {}
    for pair in instruments:
        final_dict[pair] = (data_dict[pair].pipe(data_model.pre_cleaning, instrument=pair)
                            .pipe(data_model.calculation, instrument=pair, evaluation_date=evaluation_date, begin_date=beginning_date, num_of_best_price=num_best_price)
                            .pipe(data_model.trigger_alerts))
