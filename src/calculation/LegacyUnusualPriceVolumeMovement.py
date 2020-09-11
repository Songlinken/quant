import dask.dataframe as dkd
import datetime
import numpy as np
import os
import pandas as pd
import warnings

from sys import _getframe

from src.calculation.CalculationNode import CalculationNode
from src.data_models.SmartsCsvDataModel import SmartsCsvDataModel
from src.utility.GeneralUtility import timer

warnings.simplefilter(action='ignore', category=FutureWarning)


class UnusualPriceVolumeMovement(CalculationNode):
    """
    This class is designed to trigger same alters as SMARTS does.

    Attention: class will always process one more day's data with the specified date range to ensure rolling
               calculation is correct.
    """
    def initialize(self, **kwargs):
        """
        Initialize Pointers for get_value_offset and other necessary arguments
        """
        self.arguments = {'home': os.path.expanduser('~'),
                          'class_name': self.__class__.__name__,
                          'tail_ptr': 0,
                          'head_ptr': 0,
                          'offset_index': None,
                          'offset_value': None,
                          'offset_time': None,
                          'evaluation_date': None,
                          'begin_date': None}

        self.arguments.update(kwargs)

        return self

    def get_data(self, instrument, evaluation_date, begin_date, download_data=True, unzip=True, force=False, use_cache=True):
        """
        Pull data from local machine, otherwise download data from datalab

        :param instrument: str or list (Example: 'BTCUSD' or ['BTCUSD', 'LTCUSD'])
        :param evaluation_date: date
        :param begin_date: date
        :param download_data: bool
        :param unzip: bool
        :param force: bool (override downloaded csv if existing)
        :param use_cache: bool
        :return: dict with dask dataframe (empty pandas dataframe if cache data includes all data needed)
        """
        # preserve arguments for cache calc result
        self.arguments['evaluation_date'] = evaluation_date
        self.arguments['begin_date'] = begin_date

        if use_cache:
            dk_data_dict = {}

            if isinstance(instrument, str):
                instrument = [instrument]

            for pair in instrument:
                cache_data = self.get_cache_result(instrument=pair)

                if cache_data.empty:
                    # full evaluation if no cache result found
                    dk_data_dict.update(SmartsCsvDataModel(download_data=download_data)
                                        .evaluate(instrument=pair,
                                                  evaluation_date=evaluation_date,
                                                  begin_date=begin_date - datetime.timedelta(days=1),
                                                  unzip=unzip,
                                                  force=force))
                else:
                    cache_date_begin = cache_data['event_date'].min().date()
                    cache_date_end = cache_data['event_date'].max().date()
                    self.arguments.update({'cache_date_begin_{pair}'.format(pair=pair): cache_date_begin})
                    self.arguments.update({'cache_date_end_{pair}'.format(pair=pair): cache_date_end})

                    if evaluation_date > cache_date_end:
                        if begin_date >= cache_date_begin:
                            # case_1: lower bound beyond earliest date & upper bound beyond latest date
                            self.arguments.update({'cache_case_{pair}'.format(pair=pair): 'case_1'})

                            dk_data_dict.update(SmartsCsvDataModel(download_data=download_data)
                                                .evaluate(instrument=pair,
                                                          evaluation_date=evaluation_date,
                                                          begin_date=cache_date_end,
                                                          unzip=unzip,
                                                          force=force))

                        else:
                            # case_2: lower bound before earliest date & upper bound beyond latest date
                            self.arguments.update({'cache_case_{pair}'.format(pair=pair): 'case_2'})

                            dk_data_upper_bound = (SmartsCsvDataModel(download_data=download_data)
                                                   .evaluate(instrument=pair,
                                                             evaluation_date=evaluation_date,
                                                             begin_date=cache_date_end,
                                                             unzip=unzip,
                                                             force=force))

                            dk_data_lower_bound = (SmartsCsvDataModel(download_data=download_data)
                                                   .evaluate(instrument=pair,
                                                             evaluation_date=cache_date_begin - datetime.timedelta(days=1),
                                                             begin_date=begin_date - datetime.timedelta(days=1),
                                                             unzip=unzip,
                                                             force=force))

                            dk_merged_data = dkd.concat([dk_data_upper_bound[pair], dk_data_lower_bound[pair]])

                            dk_data_dict.update({pair: dk_merged_data})

                    elif cache_date_end >= evaluation_date >= cache_date_begin:
                        if begin_date >= cache_date_begin:
                            # case_3: lower bound & upper bound within cached date range
                            self.arguments.update({'cache_case_{pair}'.format(pair=pair): 'case_3'})
                            dk_data_dict.update({pair: pd.DataFrame()})

                        else:
                            # case_4: lower bound before earliest date & upper bound within cached date range
                            self.arguments.update({'cache_case_{pair}'.format(pair=pair): 'case_4'})

                            dk_data_dict.update(SmartsCsvDataModel(download_data=download_data)
                                                .evaluate(instrument=pair,
                                                          evaluation_date=cache_date_begin - datetime.timedelta(days=1),
                                                          begin_date=begin_date - datetime.timedelta(days=1),
                                                          unzip=unzip,
                                                          force=force))

                    else:
                        # case_5: lower bound & upper bound before earliest date
                        self.arguments.update({'cache_case_{pair}'.format(pair=pair): 'case_5'})

                        dk_data_dict.update(SmartsCsvDataModel(download_data=download_data)
                                            .evaluate(instrument=pair,
                                                      evaluation_date=cache_date_begin - datetime.timedelta(days=1),
                                                      begin_date=begin_date - datetime.timedelta(days=1),
                                                      unzip=unzip,
                                                      force=force))

        else:
            dk_data_dict = (SmartsCsvDataModel(download_data=download_data)
                            .evaluate(instrument=instrument,
                                      evaluation_date=evaluation_date,
                                      begin_date=begin_date - datetime.timedelta(days=1),
                                      unzip=unzip,
                                      force=force))

        return dk_data_dict

    @timer
    def pre_cleaning(self, dk_data_set, instrument, returned_columns=None):
        """
        Necessary data type mapping, column rename and other data cleaning process

        :param dk_data_set: dask dataframe
        :param instrument: str
        :param returned_columns: dict or list
        :return: pd dataframe
        """
        if isinstance(dk_data_set, pd.DataFrame) and dk_data_set.empty:
            print('No data of {instrument} was found to do {fun}.'.format(instrument=instrument, fun=_getframe().f_code.co_name))
            return pd.DataFrame()

        # expect 'Event Date', 'Event Time' & 'Event Millis' in dataframe
        dk_data_set['DateTime'] = (dk_data_set['Event Date'] + ' ' + dk_data_set['Event Time'] + '.' + dk_data_set['Event Millis'].astype(str)).astype('datetime64[ns]')
        dk_data_set['Event Date'] = dk_data_set['Event Date'].astype('datetime64[ns]')

        # Todo: Have all columns with right pandas data type, type converting is slow here
        # data_set['Event Time'] = pd.to_datetime(data_set['Event Time'], format='%H:%M:%S').dt.time

        # get market data (look at either buy or sell side)
        dk_market_data = dk_data_set.loc[(dk_data_set['Event Type'] == 'Fill') & (dk_data_set['Side'] == 'buy')]

        # sub_set with necessary columns
        trade_in = instrument[:3]
        trade_to = instrument[-3:]

        if returned_columns is None:
            returned_columns = {
                'Account ID': 'account_id',
                'DateTime': 'date_time',
                'Event ID': 'event_id',
                'Event Date': 'event_date',
                'Event Time': 'event_time',
                'Event Type': 'event_type',
                'Side': 'side',
                'Symbol': 'symbol',
                'Fill Price ({trade_to})'.format(trade_to=trade_to): 'price',
                'Fill Quantity ({trade_in})'.format(trade_in=trade_in): 'quantity'
            }

            dk_sub_market_data = dk_market_data.rename(columns=returned_columns)
            dk_sub_market_data = dk_sub_market_data[list(returned_columns.values())]

        elif isinstance(returned_columns, list):
            dk_sub_market_data = dk_market_data[returned_columns]

        elif isinstance(returned_columns, dict):
            dk_sub_market_data = dk_market_data.rename(columns=returned_columns)
            dk_sub_market_data = dk_sub_market_data[list(returned_columns.values())]

        else:
            raise TypeError('Wrong type for returned_columns')

        result = dk_sub_market_data.compute()

        if result.empty:
            raise ValueError('No data was found after {fun}'.format(fun=_getframe().f_code.co_name))

        return result

    @timer
    def calculation(self, data_set, instrument, offset_time, npartitions=24, cache_result=True):
        """
        Calculate all necessary columns for alert_analysis, such as rolling sum, pct_diff

        Attention: offset_time cannot be greater than 1 day

        :param data_set: pd dataframe
        :param instrument: str
        :param offset_time: int
        :param npartitions: int
        :param cache_result: bool
        :return: pd dataframe
        """
        if data_set.empty:
            print('No data of {instrument} was found to do {fun}.\nUse cached data.'
                  .format(instrument=instrument, fun=_getframe().f_code.co_name))

            calculation_result = self.get_cache_result(instrument=instrument)
            date_filter = ((calculation_result['event_date'] >= self.arguments['begin_date'])
                           & (calculation_result['event_date'] <= self.arguments['evaluation_date']))

            return calculation_result.loc[date_filter]

        # preserve original index before calculate value offset
        data_set = data_set.sort_values('date_time')
        data_set = data_set.reset_index()

        # calculate value offset
        self.arguments['offset_index'] = data_set['date_time']
        self.arguments['offset_value'] = data_set['fill_price']

        if not isinstance(offset_time, list):
            offset_time = [offset_time]

        for time in offset_time:
            offset_price = 'price_{time}m_ago'.format(time=time)
            self.arguments['offset_time'] = pd.Timedelta(time, unit='m')

            # get_value_offset is a dual pointer function, which is not suitable for parallelism
            data_set[offset_price] = data_set['date_time'].map(self.get_value_offset)

            # clean up for next loop
            self.arguments['head_ptr'] = 0
            self.arguments['tail_ptr'] = 0

        # convert to dask dataframe for other logic calculations, set partition=1 when partition is too small
        if data_set.shape[0] < 100000:
            dk_data_set = dkd.from_pandas(data_set, npartitions=1)

        else:
            dk_data_set = dkd.from_pandas(data_set, npartitions=npartitions)
            
        for time in offset_time:
            offset_price = 'price_{time}m_ago'.format(time=time)
            offset_volume = 'quantity_sum_past_{time}m'.format(time=time)

            # calculate rolling quantity
            dk_data_set = dk_data_set.set_index('date_time')
            dk_data_set[offset_volume] = dk_data_set['quantity'].rolling('{time}min'.format(time=time)).sum()
            dk_data_set = dk_data_set.reset_index()

            # trade to offset calc
            dk_data_set['price_diff_{time}m'.format(time=time)] = dk_data_set['price'] - dk_data_set[offset_price]
            dk_data_set['abs_price_diff_{time}m'.format(time=time)] = dk_data_set['price_diff_{time}m'.format(time=time)].abs()
            dk_data_set['price_pct_change_over_{time}m'.format(time=time)] = (dk_data_set['price'] / dk_data_set[offset_price]) - 1
            dk_data_set['abs_price_pct_change_over_{time}m'.format(time=time)] = dk_data_set['price_pct_change_over_{time}m'.format(time=time)].abs()

        # trade to trade calc
        dk_data_set['price_diff_t2t'] = dk_data_set['price'].diff()
        dk_data_set['abs_price_diff_t2t'] = dk_data_set['price_diff_t2t'].abs()
        dk_data_set['price_pct_change_t2t'] = dk_data_set['price_diff_t2t'] / dk_data_set['price'].shift(1)
        dk_data_set['abs_price_pct_change_t2t'] = dk_data_set['price_pct_change_t2t'].abs()

        calculation_result = dk_data_set.set_index('index')

        calculation_result = calculation_result.compute().sort_values('date_time')

        # concatenate new data to cache data
        cache_data = self.get_cache_result(instrument=instrument)

        if not cache_data.empty:
            cache_case = self.arguments['cache_case_{pair}'.format(pair=instrument)]

            # check the cache case for caching result
            if cache_case == 'case_1':
                merge_date_filter = self.arguments['cache_date_end_{pair}'.format(pair=instrument)]
                calculation_result = calculation_result.loc[calculation_result['event_date'] != merge_date_filter]
                calculation_result = pd.concat([cache_data, calculation_result]).sort_values('date_time')

            elif cache_case == 'case_2':
                merge_date_filter_upper = self.arguments['cache_date_end_{pair}'.format(pair=instrument)]
                merge_date_filter_lower = self.arguments['begin_date'] - datetime.timedelta(days=1)
                calculation_result = calculation_result.loc[(calculation_result['event_date'] != merge_date_filter_upper)
                                                            & (calculation_result['event_date'] != merge_date_filter_lower)]
                calculation_result = pd.concat([cache_data, calculation_result]).sort_values('date_time')

            elif cache_case == 'case_4':
                merge_date_filter = self.arguments['begin_date'] - datetime.timedelta(days=1)
                calculation_result = calculation_result.loc[calculation_result['event_date'] != merge_date_filter]
                calculation_result = pd.concat([cache_data, calculation_result]).sort_values('date_time')

            elif cache_case == 'case_5':
                merge_date_filter = self.arguments['begin_date'] - datetime.timedelta(days=1)
                calculation_result = calculation_result.loc[calculation_result['event_date'] != merge_date_filter]
                calculation_result = pd.concat([cache_data, calculation_result]).sort_values('date_time')

        else:
            calculation_result = calculation_result.loc[calculation_result['event_date'] >= self.arguments['begin_date']]

        if cache_result:
            self.cache_result(calculation_result, instrument=instrument)

        date_filter = ((calculation_result['event_date'] >= self.arguments['begin_date'])
                       & (calculation_result['event_date'] <= self.arguments['evaluation_date']))

        return calculation_result[date_filter]

    def get_value_offset(self, value):
        final_val = np.nan

        for i in range(self.arguments['tail_ptr'], self.arguments['head_ptr'] - 1):
            if value - self.arguments['offset_index'][self.arguments['tail_ptr']] > self.arguments['offset_time']:
                final_val = self.arguments['offset_value'][self.arguments['tail_ptr']]
                self.arguments['tail_ptr'] += 1

            else:
                break

        self.arguments['head_ptr'] += 1

        if final_val is not np.nan:
            self.arguments['tail_ptr'] -= 1

        return final_val

    @timer
    def get_cache_result(self, instrument):
        local_path = (self.arguments['home'] + '/Documents/Calculation_result/{class_name}/{instrument}/'
                      .format(class_name=self.arguments['class_name'], instrument=instrument))

        if not os.path.exists(local_path):
            print('No cache result for {instrument}'.format(instrument=instrument))
            return pd.DataFrame()

        else:
            cache_files = [file for file in os.listdir(local_path) if file.startswith(instrument)]

            if cache_files:
                cache_files.sort(key=lambda fn: os.path.getmtime(local_path + fn), reverse=True)
                most_recent_file = cache_files[0]
                calculation_result = pd.read_pickle(local_path + most_recent_file)

            else:
                print('No cache result for {instrument}'.format(instrument=instrument))
                calculation_result = pd.DataFrame()

            return calculation_result

    @timer
    def cache_result(self, data_set, instrument):

        local_path = (self.arguments['home'] + '/Documents/Calculation_result/{class_name}/{instrument}/'
                      .format(class_name=self.arguments['class_name'], instrument=instrument))

        if not os.path.exists(local_path):
            os.makedirs(local_path)

        max_date = data_set['event_date'].max().date()
        min_date = data_set['event_date'].min().date()

        data_set.to_pickle(local_path + '{instrument}_{class_name}_{min_date}_to_{max_date}.pkl'
                           .format(instrument=instrument, class_name=self.arguments['class_name'], min_date=min_date, max_date=max_date))

    # def trigger_alerts(self, data_set, account_id, trigger_time):
    #     # price that triggers unusual price movement
    #     trigger_price = data_set.loc[(data_set['DateTime'] == trigger_time) & (data_set['Account ID'] == account_id)]
    #     trigger_price_index = trigger_price.index.tolist()[0]
    #
    #     # find the previous price before trigger_price
    #     index_before_trigger = data_set.ix[:trigger_price_index].index.tolist()
    #     index_before_trigger.reverse()
    #
    #     for index in index_before_trigger:
    #
    #         if data_set.ix[trigger_price_index, 'Event ID'] != data_set.ix[index, 'Event ID']:
    #             previous_price = data_set.ix[index]
    #             break
    #
    #     # price change calculations
    #     first_price = previous_price['Fill Price (USD)']
    #     second_price = trigger_price.ix[trigger_price_index, 'Fill Price (USD)']
    #     price_change = round(second_price - first_price, 2)
    #     percentage_change = round((price_change / first_price) * 100, 2)
    #
    #     print('Price Change trade to trade is ${price_change} ({percentage_change}%) from ${first_price} to ${second_price}'
    #           .format(price_change=price_change, percentage_change=percentage_change, first_price=first_price, second_price=second_price))
    #
    #     return previous_price


if __name__ == '__main__':

    instruments = ['BTCUSD', 'ETHBTC', 'LTCUSD']
    evaluation_date = datetime.date(2019, 1, 3)
    beginning_date = datetime.date(2018, 12, 29)

    data_model = UnusualPriceVolumeMovement().initialize()
    data_dict = data_model.get_data(instrument=instruments, evaluation_date=evaluation_date, begin_date=beginning_date)

    # cleaning & calculation
    final_dict = {}
    for pair in instruments:
        final_dict[pair] = (data_dict[pair].pipe(data_model.pre_cleaning, instrument=pair)
                            .pipe(data_model.calculation, instrument=pair, offset_time=[10, 60]))
