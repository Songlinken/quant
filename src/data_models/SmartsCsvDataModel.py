import pandas as pd

from sys import _getframe

from src.data_models.SmartsDataModel import SmartsDataModel
from src.utility.GeneralUtility import timer
from src.utility.SmartsDataUtility import download_smarts_data, read_csv_to_dk_dataframe


class SmartsCsvDataModel(object):
    """
    Special data model does not query database.
    """
    def __init__(self, download_data=False):

        self.download_data = download_data

    def evaluate(self, instrument, evaluation_date, begin_date, unzip=True, force=False, use_db=True):
        """
        :param instrument: str or list (Example: 'BTCUSD' or ['BTCUSD', 'LTCUSD'])
        :param evaluation_date: date
        :param begin_date: date
        :param unzip: bool
        :param force: bool (override downloaded csv if existing)
        :param use_db: bool
        :return: dict with pd dataframe
        """
        if evaluation_date < begin_date:
            raise ValueError('evaluation_date must be greater than begin_date.')

        if isinstance(instrument, str):
            instrument = [instrument]

        if use_db:
            pd_data_frame_dict = {}
            for pair in instrument:
                data_set = SmartsDataModel().initialize(
                    start_date=begin_date,
                    evaluation_date=evaluation_date,
                    other_condition="symbol = '{}'".format(pair)
                ).evaluate()

                pd_data_frame_dict.update({pair: data_set})

            return pd_data_frame_dict

        if self.download_data:
            download_smarts_data(instrument=instrument, evaluation_date=evaluation_date, begin_date=begin_date, unzip=unzip, force=force)
            dk_data_frame_dict = read_csv_to_dk_dataframe(instrument=instrument, evaluation_date=evaluation_date, begin_date=begin_date)

        else:
            dk_data_frame_dict = read_csv_to_dk_dataframe(instrument=instrument, evaluation_date=evaluation_date, begin_date=begin_date)

        pd_data_frame_dict = {}
        for pair in instrument:
            data_set = self.pre_cleaning(dk_data_frame_dict[pair], pair)

            pd_data_frame_dict.update({pair: data_set})

        return pd_data_frame_dict

    @timer
    def pre_cleaning(self, dk_data_set, instrument, returned_columns=None):
        """
        Necessary data type mapping, column rename and other data cleaning process.

        :param dk_data_set: dask dataframe
        :param instrument: str
        :param returned_columns: dict or list
        :return: pd dataframe
        """
        # expect 'Event Date', 'Event Time' & 'Event Millis' in dataframe
        dk_data_set['DateTime'] = dk_data_set['Event Date'] + ' ' + dk_data_set['Event Time'] + '.' + dk_data_set['Event Millis'].astype(str).str.zfill(3)

        # sub_set with necessary columns
        trade_in = instrument[:3]
        trade_to = instrument[-3:]

        if returned_columns is None:
            returned_columns = {
                'Account ID': 'account_id',
                'Auction ID': 'auction_id',
                'DateTime': 'date_time',
                'Event ID': 'event_id',
                'Event Date': 'event_date',
                'Event Time': 'event_time',
                'Event Millis': 'event_millis',
                'Event Type': 'event_type',
                'Execution Options': 'execution_options',
                'Order ID': 'order_id',
                'Order Type': 'order_type',
                'Side': 'side',
                'Symbol': 'symbol',
                'Limit Price ({trade_to})'.format(trade_to=trade_to): 'limit_price',
                'Original Quantity ({trade_in})'.format(trade_in=trade_in): 'original_quantity',
                'Gross Notional Value ({trade_to})'.format(trade_to=trade_to): 'gross_notional_value',
                'Fill Price ({trade_to})'.format(trade_to=trade_to): 'fill_price',
                'Fill Quantity ({trade_in})'.format(trade_in=trade_in): 'fill_quantity',
                'Total Exec Quantity ({trade_in})'.format(trade_in=trade_in): 'total_exec_quantity',
                'Remaining Quantity ({trade_in})'.format(trade_in=trade_in): 'remaining_quantity',
                'Avg Price ({trade_to})'.format(trade_to=trade_to): 'avg_price',
                'Fees ({trade_to})'.format(trade_to=trade_to): 'fees',
                'IOI ID': 'ioi_id',
                'Order Cancel Reason': 'order_cancel_reason'
            }

            dk_market_data = dk_data_set.rename(columns=returned_columns)
            dk_market_data = dk_market_data[list(returned_columns.values())]

        elif isinstance(returned_columns, list):
            dk_market_data = dk_data_set[returned_columns]

        elif isinstance(returned_columns, dict):
            dk_market_data = dk_data_set.rename(columns=returned_columns)
            dk_market_data = dk_market_data[list(returned_columns.values())]

        else:
            raise TypeError('Wrong type for returned_columns')

        result = dk_market_data.compute().sort_values(['date_time', 'event_id']).reset_index(drop=True)

        # Todo: Have all columns with right pandas data type, type converting is slow here
        result['date_time'] = result['date_time'].astype('datetime64[ns]')
        result['event_date'] = result['event_date'].astype('datetime64[ns]')
        result['event_time'] = pd.to_datetime(result['event_time'], format='%H:%M:%S').dt.time
        result['event_millis'] = result['event_millis'].astype('int')
        result['event_millis'] = pd.to_timedelta(result['event_millis'], unit='ms')
        result['event_millis'] = pd.to_datetime(result['event_millis'].dt.total_seconds(), unit='s')
        result['event_millis'] = result['event_millis'].dt.time

        if result.empty:
            raise ValueError('No data was found after {fun}'.format(fun=_getframe().f_code.co_name))

        return result
