import datetime
import numpy as np
import pandas as pd

from src.calculation.CalculationNode import CalculationNode
from src.data_models.AccountDerivedMetaDataModel import AccountDerivedMetaDataModel
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.data_models.SmartsDataModel import SmartsDataModel
from src.data_models.OrderFillEventDataModel import OrderFillEventDataModel
from src.utility.GeneralUtility import timer


class MarketMakerIdentificationMatrix(CalculationNode):
    """
    This class designed to calculate necessary matrix for market marker identification.
    """
    @timer
    def get_data(self, instrument, evaluation_date):
        """
        Pull necessary data from interim database. Only applies on one day per time.

        :param instrument: str (Example: 'BTCUSD')
        :param evaluation_date: date
        :return: pd dataframe
        """
        base_data_set = (SmartsDataModel()
                         .initialize(evaluation_date=evaluation_date,
                                     other_condition="symbol = '{}'".format(instrument))
                         .evaluate())

        order_fill_events = (OrderFillEventDataModel()
                             .initialize(evaluation_date=evaluation_date,
                                         other_condition="trading_pair = '{}'".format(instrument))
                             .evaluate()
                             .sort_values('order_fill_event_key'))

        # drop gemini liquidity data
        order_fill_events = order_fill_events.loc[~order_fill_events['client_order_id'].isin(['Mobile', 'Web', 'Recurring'])]

        unique_account_ids = base_data_set['account_id'].unique().tolist()
        account_info = (AccountDerivedMetaDataModel()
                        .initialize(other_condition='exchange_account_id in {}'.format(tuple(unique_account_ids)))
                        .evaluate())

        # merge data sets
        final_data_set = (base_data_set.merge(order_fill_events[['event_id', 'order_id', 'liquidity_indicator']],
                                              how='left',
                                              on=['event_id', 'order_id'])
                                       .merge(account_info[['exchange_account_id', 'is_institutional']],
                                              how='left',
                                              left_on='account_id',
                                              right_on='exchange_account_id'))

        final_data_set['date_time'] = pd.to_datetime(final_data_set['event_date'].astype('str') + ' ' + final_data_set['event_time'].astype(str)) + final_data_set['event_millis']

        return final_data_set

    @timer
    def get_place_cancel_interval(self, data_set):
        """
        Calculate the time difference between place & cancel for an order.

        :param data_set: pd dataframe
        :return: pd dataframe
        """
        data_set = data_set.reset_index()
        data_set = data_set.assign(place_cancel_time_diff=np.nan)

        columns = data_set.columns
        data_types = data_set.dtypes
        data_array = data_set.to_numpy()

        account_order_place_time_dict = {}
        for row in range(1, data_array.shape[0]):
            # column 7 is event_type
            if data_array[row, 7] in ['Initial', 'Place']:
                # column 9 is order_id, column -2 is date_time
                account_order_place_time_dict[data_array[row, 9]] = data_array[row, -2]

            elif data_array[row, 7] == 'Cancel':
                # column -1 is place_cancel_time_diff
                data_array[row, -1] = (data_array[row, -2] - account_order_place_time_dict[data_array[row, 9]]).total_seconds()

        result_data_set = pd.DataFrame(np.row_stack(data_array), columns=columns)
        result_data_set = result_data_set.astype(data_types)
        result_data_set = result_data_set.set_index('index')
        result_data_set.index.name = None

        return result_data_set

    @timer
    def get_bid_ask_ratio(self, data_set):
        """
        Calculate the bid vs ask ratio for each of accounts.

        :param data_set: pd dataframe
        :return: pd dataframe
        """
        data_set = data_set.reset_index()
        data_set = data_set.assign(bid_ask_ratio=np.nan)

        columns = data_set.columns
        data_types = data_set.dtypes
        data_array = data_set.to_numpy()

        account_bid_ask_dict = {}
        for row in range(1, data_array.shape[0]):
            # column 7 is event_type
            if data_array[row, 7] in ['Initial', 'Place']:
                # column 1 is account_id, column 11 is side
                if (data_array[row, 1], data_array[row, 11]) in account_bid_ask_dict:
                    # column 14 is original_quantity_crypto
                    account_bid_ask_dict[data_array[row, 1], data_array[row, 11]] = round(account_bid_ask_dict[data_array[row, 1], data_array[row, 11]] + data_array[row, 14], 10)
                else:
                    account_bid_ask_dict[data_array[row, 1], data_array[row, 11]] = round(data_array[row, 14], 10)

            elif data_array[row, 7] == 'Fill':
                # column 17 is fill_quantity_crypto
                account_bid_ask_dict[data_array[row, 1], data_array[row, 11]] = round(account_bid_ask_dict[data_array[row, 1], data_array[row, 11]] - data_array[row, 17], 10)

                if account_bid_ask_dict[data_array[row, 1], data_array[row, 11]] == 0:
                    del account_bid_ask_dict[data_array[row, 1], data_array[row, 11]]

            else:
                # column 19 is remaining_quantity_crypto
                account_bid_ask_dict[data_array[row, 1], data_array[row, 11]] = round(account_bid_ask_dict[data_array[row, 1], data_array[row, 11]] - data_array[row, 19], 10)

                if account_bid_ask_dict[data_array[row, 1], data_array[row, 11]] == 0:
                    del account_bid_ask_dict[data_array[row, 1], data_array[row, 11]]

            if ((data_array[row, 1], 'buy') in account_bid_ask_dict) and ((data_array[row, 1], 'sell') in account_bid_ask_dict):
                # column -1 is bid_ask_ratio
                data_array[row, -1] = account_bid_ask_dict[(data_array[row, 1], 'buy')] / account_bid_ask_dict[(data_array[row, 1], 'sell')]

            elif ((data_array[row, 1], 'buy') in account_bid_ask_dict) or ((data_array[row, 1], 'sell') in account_bid_ask_dict):
                # fill large number of bid_ask_ratio to represent Inf
                data_array[row, -1] = 10000

            else:
                data_array[row, -1] = np.nan

        result_data_set = pd.DataFrame(np.row_stack(data_array), columns=columns)
        result_data_set = result_data_set.astype(data_types)
        result_data_set = result_data_set.set_index('index')
        result_data_set.index.name = None

        return result_data_set

    @timer
    def evaluate(self, instrument, evaluation_date):
        data_set = self.get_data(instrument, evaluation_date)

        final_data_set = (data_set.pipe(self.get_place_cancel_interval)
                          .pipe(self.get_bid_ask_ratio))

        total_order_per_account = (final_data_set.loc[final_data_set['event_type'].isin(['Initial', 'Place'])]
                                                 .groupby('account_id', as_index=False)['order_id']
                                                 .count()
                                                 .rename(columns={'order_id': 'total_orders'}))

        fill_buy_order_per_account = (final_data_set.loc[(final_data_set['event_type'] == 'Fill') & (final_data_set['side'] == 'buy')]
                                                    .groupby('account_id', as_index=False)['order_id']
                                                    .count()
                                                    .rename(columns={'order_id': 'fill_buy_orders_count'}))

        fill_sell_order_per_account = (final_data_set.loc[(final_data_set['event_type'] == 'Fill') & (final_data_set['side'] == 'sell')]
                                                     .groupby('account_id', as_index=False)['order_id']
                                                     .count()
                                                     .rename(columns={'order_id': 'fill_sell_orders_count'}))

        fill_buy_order_quant_per_account = (final_data_set.loc[(final_data_set['event_type'] == 'Fill') & (final_data_set['side'] == 'buy')]
                                                          .groupby('account_id', as_index=False)['fill_quantity_crypto']
                                                          .sum()
                                                          .rename(columns={'fill_quantity_crypto': 'fill_buy_orders_quantity'}))

        fill_sell_order_quant_per_account = (final_data_set.loc[(final_data_set['event_type'] == 'Fill') & (final_data_set['side'] == 'sell')]
                                                           .groupby('account_id', as_index=False)['fill_quantity_crypto']
                                                           .sum()
                                                           .rename(columns={'fill_quantity_crypto': 'fill_sell_orders_quantity'}))

        cancel_order_per_account = (final_data_set.loc[final_data_set['event_type'] == 'Cancel']
                                                  .groupby('account_id', as_index=False)['order_id']
                                                  .count()
                                                  .rename(columns={'order_id': 'cancel_orders'}))

        maker_count_per_account = (final_data_set.loc[final_data_set['liquidity_indicator'] == 'maker']
                                                 .groupby('account_id', as_index=False)['order_id']
                                                 .count()
                                                 .rename(columns={'order_id': 'maker_counts'}))

        taker_count_per_account = (final_data_set.loc[final_data_set['liquidity_indicator'] == 'taker']
                                                 .groupby('account_id', as_index=False)['order_id']
                                                 .count()
                                                 .rename(columns={'order_id': 'taker_counts'}))

        bid_ask_ratio_median_per_account = (final_data_set.groupby('account_id', as_index=False)['bid_ask_ratio']
                                                          .median()
                                                          .rename(columns={'bid_ask_ratio': 'bid_ask_ratio_median'}))

        final_data_set_agg = (final_data_set.groupby(['account_id', 'data_from_date', 'symbol'], as_index=False)
                                            .agg({'place_cancel_time_diff': 'median',
                                                  'bid_ask_ratio': 'std',
                                                  'is_institutional': 'mean'})
                                            .rename(columns={'bid_ask_ratio': 'bid_ask_ratio_std'})
                                            .merge(total_order_per_account, on='account_id', how='left')
                                            .merge(fill_buy_order_per_account, on='account_id', how='left')
                                            .merge(fill_sell_order_per_account, on='account_id', how='left')
                                            .merge(fill_buy_order_quant_per_account, on='account_id', how='left')
                                            .merge(fill_sell_order_quant_per_account, on='account_id', how='left')
                                            .merge(cancel_order_per_account, on='account_id', how='left')
                                            .merge(maker_count_per_account, on='account_id', how='left')
                                            .merge(taker_count_per_account, on='account_id', how='left')
                                            .merge(bid_ask_ratio_median_per_account, on='account_id', how='left'))

        final_data_set_agg['data_from_date'] = pd.to_datetime(evaluation_date)
        final_data_set_agg['symbol'] = instrument

        return final_data_set_agg


# sample use case of this class
if __name__ == '__main__':
    calculation_module = MarketMakerIdentificationMatrix()

    instruments = CryptoPairsDataModel().evaluate()['trading_pair'].tolist()
    evaluation_date = datetime.date(2019, 1, 1)

    data_set_list = []
    for instrument in instruments:
        raw_data = calculation_module.evaluate(instrument, evaluation_date)

    final_result = pd.concat(data_set_list)
