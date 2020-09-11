import dask.dataframe as dkd
import datetime
import numpy as np
import os
import pandas as pd
import warnings

from src.calculation.CalculationNode import CalculationNode
from src.data_models.SmartsDataModel import SmartsDataModel
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

    def get_data(self, instrument, evaluation_date, begin_date):
        """
        Pull data from interim database, otherwise download data from datalab

        :param instrument: str (Example: 'BTCUSD')
        :param evaluation_date: date
        :param begin_date: date
        :return: pd dataframe
        """
        data_set = SmartsDataModel().initialize(evaluation_date=evaluation_date, begin_date=begin_date, other_condition="symbol = '{}'".format(instrument)).evaluate()
        data_set['date_time'] = pd.to_datetime(data_set['event_date'].astype('str') + ' ' + data_set['event_time'].astype(str)) + data_set['event_millis']

        return data_set

    @timer
    def calculation(self, data_set, instrument, offset_time, npartitions=24):
        """
        Calculate all necessary columns for alert_analysis, such as rolling sum, pct_diff

        Attention: offset_time cannot be greater than 1 day

        :param data_set: pd dataframe
        :param instrument: str
        :param offset_time: int
        :param npartitions: int
        :return: pd dataframe
        """
        data_set = data_set.loc[(data_set['symbol'] == instrument) & (data_set['event_type'] == 'Fill')]

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
            dk_data_set[offset_volume] = dk_data_set['fill_quantity_crypto'].rolling('{time}min'.format(time=time)).sum()
            dk_data_set = dk_data_set.reset_index()

            # trade to offset calc
            dk_data_set['price_diff_{time}m'.format(time=time)] = dk_data_set['fill_price'] - dk_data_set[offset_price]
            dk_data_set['abs_price_diff_{time}m'.format(time=time)] = dk_data_set['price_diff_{time}m'.format(time=time)].abs()
            dk_data_set['price_pct_change_over_{time}m'.format(time=time)] = (dk_data_set['fill_price'] / dk_data_set[offset_price]) - 1
            dk_data_set['abs_price_pct_change_over_{time}m'.format(time=time)] = dk_data_set['price_pct_change_over_{time}m'.format(time=time)].abs()

        # trade to trade calc
        dk_data_set['price_diff_t2t'] = dk_data_set['fill_price'].diff()
        dk_data_set['abs_price_diff_t2t'] = dk_data_set['price_diff_t2t'].abs()
        dk_data_set['price_pct_change_t2t'] = dk_data_set['price_diff_t2t'] / dk_data_set['fill_price'].shift(1)
        dk_data_set['abs_price_pct_change_t2t'] = dk_data_set['price_pct_change_t2t'].abs()

        calculation_result = dk_data_set.set_index('index')

        calculation_result = calculation_result.compute().sort_values('date_time')

        return calculation_result

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


# sample use case for this class
if __name__ == '__main__':
    instrument = 'LTCUSD'
    evaluation_date = datetime.date(2019, 1, 3)
    beginning_date = datetime.date(2018, 12, 29)

    data_model = UnusualPriceVolumeMovement().initialize()
    data = data_model.get_data(instrument=instrument, evaluation_date=evaluation_date, begin_date=beginning_date)

    result = data_model.calculation(data_set=data, instrument=instrument, offset_time=[10])
