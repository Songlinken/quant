import datetime
import heapq
import numpy as np
import pandas as pd

from src.calculation.CalculationNode import CalculationNode
from src.data_models.SmartsDataModel import SmartsDataModel
from src.utility.GeneralUtility import timer


class DailyBestPrices(CalculationNode):
    """
    This class designed to get best prices which will be used for SMARTS bait & switch alerts.
    Calculation result could be cached into database account_daily_best_prices table if necessary.
    """
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

    def calculation(self, data_set, instrument, num_of_best_prices, include_best_volume=True, split_best_volume_to_account=False):
        """
        Get best prices of the given dataframe. Only works one day per instrument at a time.

        :param data_set: pd dataframe
        :param instrument: str
        :param num_of_best_prices: int
        :param include_best_volume: bool
        :param split_best_volume_to_account: bool (will disable include_best_volume)
        :return: pd dataframe
        """
        if not (data_set['symbol'] == instrument).all():
            raise ValueError('data_set provided does not match the instrument {}'.format(instrument))

        calculation_result = (data_set.groupby('side').apply(self.get_best_prices, num_of_best_prices)
                              .reset_index(level=0, drop=True).sort_values(['date_time', 'event_id']))

        if split_best_volume_to_account:
            calculation_result = calculation_result.groupby('side').apply(self.get_best_volumes_split_by_account, num_of_best_prices)
            calculation_result = calculation_result.sort_values(['date_time', 'event_id']).reset_index(level=0, drop=True)
            calculation_result = self.get_best_price_volume_on_the_other_side(calculation_result, num_of_best_prices)
            return calculation_result

        # add best volumes to best prices
        if include_best_volume:
            calculation_result = calculation_result.groupby('side').apply(self.get_best_volumes, num_of_best_prices)
            calculation_result = calculation_result.sort_values(['date_time', 'event_id']).reset_index(level=0, drop=True)

        return calculation_result

    @timer
    def get_best_prices(self, data_set, num_of_best_prices):
        """
        Calculate number of best prices for each timestamp. Data must be sorted with ascending time before running this function.
        Data columns must be in the same order as the csv files from datalab. No auction/block trading data has best prices.

        :param data_set: pd dataframe
        :param num_of_best_prices: int
        :return: pd dataframe
        """
        if num_of_best_prices < 1 or not isinstance(num_of_best_prices, int):
            raise ValueError('num_of_best_prices must be an integer equal or greater than 1.')

        if not data_set['side'].duplicated()[1:].all():
            raise ValueError('Side can only be either buy or sell.')

        if not data_set.loc[data_set['event_type'] != 'Initial', 'event_date'].duplicated()[1:].all():
            raise ValueError('Function only works for one day at a time.')

        # initialize new best price columns
        data_set = data_set.reset_index()
        original_columns = data_set.columns.tolist()
        best_prices = ['best_price_{}'.format(num) for num in range(1, num_of_best_prices + 1)]
        best_prices_dict = {key_: np.nan for key_ in best_prices}
        data_set = data_set.assign(**best_prices_dict)
        data_set = data_set[original_columns + best_prices]
        columns = data_set.columns
        data_types = data_set.dtypes

        # loop array to speed up instead of itertuples
        data_array = data_set.to_numpy()
        best_price = []
        result = []

        for row in range(data_array.shape[0]):
            # column 8 is execution_options & column 10 is order_type
            if data_array[row, 8] in ['auction-only', 'block', 'indication-of-interest'] or data_array[row, 10] == 'market':
                result.append(data_array[row])

            else:
                # column 7 should be event_type
                if data_array[row, 7] in ['Initial', 'Place']:

                    # column 13 should be limit_price
                    best_price.append(data_array[row, 13])

                    # column 11 should be side
                    if data_array[row, 11] == 'buy':

                        unique_prices = heapq.nlargest(num_of_best_prices, list(set(best_price)), lambda x: -np.inf if x is np.nan else x)
                        unique_prices.extend(np.nan for _ in range(num_of_best_prices - len(unique_prices)))
                        data_array[row, -num_of_best_prices:] = unique_prices[0: num_of_best_prices]
                        result.append(data_array[row])

                    else:
                        unique_prices = heapq.nsmallest(num_of_best_prices, list(set(best_price)), lambda x: np.inf if x is np.nan else x)
                        unique_prices.extend(np.nan for _ in range(num_of_best_prices - len(unique_prices)))
                        data_array[row, -num_of_best_prices:] = unique_prices[0: num_of_best_prices]
                        result.append(data_array[row])

                # column 7 should be event_type & column 19 should be remaining_quantity
                elif data_array[row, 7] == 'Fill' and data_array[row, 19] == 0:

                    # column 13 should be limit_price
                    best_price.remove(data_array[row, 13])

                    # column 11 should be side
                    if data_array[row, 11] == 'buy':
                        unique_prices = heapq.nlargest(num_of_best_prices, list(set(best_price)), lambda x: -np.inf if x is np.nan else x)
                        unique_prices.extend(np.nan for _ in range(num_of_best_prices - len(unique_prices)))
                        data_array[row, -num_of_best_prices:] = unique_prices[0: num_of_best_prices]
                        result.append(data_array[row])

                    else:
                        unique_prices = heapq.nsmallest(num_of_best_prices, list(set(best_price)), lambda x: np.inf if x is np.nan else x)
                        unique_prices.extend(np.nan for _ in range(num_of_best_prices - len(unique_prices)))
                        data_array[row, -num_of_best_prices:] = unique_prices[0: num_of_best_prices]
                        result.append(data_array[row])

                elif data_array[row, 7] == 'Fill' and data_array[row, 19] > 0:
                    if data_array[row, 11] == 'buy':
                        unique_prices = heapq.nlargest(num_of_best_prices, list(set(best_price)), lambda x: -np.inf if x is np.nan else x)
                        unique_prices.extend(np.nan for _ in range(num_of_best_prices - len(unique_prices)))
                        data_array[row, -num_of_best_prices:] = unique_prices[0: num_of_best_prices]
                        result.append(data_array[row])

                    else:
                        unique_prices = heapq.nsmallest(num_of_best_prices, list(set(best_price)), lambda x: np.inf if x is np.nan else x)
                        unique_prices.extend(np.nan for _ in range(num_of_best_prices - len(unique_prices)))
                        data_array[row, -num_of_best_prices:] = unique_prices[0: num_of_best_prices]
                        result.append(data_array[row])

                elif data_array[row, 7] == 'Cancel':
                    best_price.remove(data_array[row, 13])

                    if data_array[row, 11] == 'buy':
                        unique_prices = heapq.nlargest(num_of_best_prices, list(set(best_price)), lambda x: -np.inf if x is np.nan else x)
                        unique_prices.extend(np.nan for _ in range(num_of_best_prices - len(unique_prices)))
                        data_array[row, -num_of_best_prices:] = unique_prices[0: num_of_best_prices]
                        result.append(data_array[row])
                    else:
                        unique_prices = heapq.nsmallest(num_of_best_prices, list(set(best_price)), lambda x: np.inf if x is np.nan else x)
                        unique_prices.extend(np.nan for _ in range(num_of_best_prices - len(unique_prices)))
                        data_array[row, -num_of_best_prices:] = unique_prices[0: num_of_best_prices]
                        result.append(data_array[row])

        result_data_set = pd.DataFrame(np.row_stack(result), columns=columns)
        result_data_set = result_data_set.astype(data_types)
        result_data_set = result_data_set.set_index('index')
        result_data_set.index.name = None

        return result_data_set

    @timer
    def get_best_volumes(self, data_set, num_of_best_prices):
        """
        Get the available volume of best prices. Need to have best prices first.

        :param data_set: pd dataframe
        :param num_of_best_prices: int
        :return: pd dataframe
        """
        if not data_set['side'].duplicated()[1:].all():
            raise ValueError('Side can only be either buy or sell.')

        # initialize price & volume dict
        price_list = data_set['limit_price'].value_counts().keys().tolist()
        initial_volume = [0] * len(price_list)
        price_volume_dict = dict(zip(price_list, initial_volume))

        # initialize new best volume columns
        data_set = data_set.reset_index()
        original_columns = data_set.columns.tolist()
        best_volumes = ['best_volume_{}'.format(num) for num in range(1, num_of_best_prices + 1)]
        best_volumes_dict = {key_: np.nan for key_ in best_volumes}
        data_set = data_set.assign(**best_volumes_dict)
        data_set = data_set[original_columns + best_volumes]
        columns = data_set.columns
        data_types = data_set.dtypes
        data = data_set.to_numpy()

        for row in range(data.shape[0]):
            # column 8 is execution_options & column 10 is order_type
            if data[row, 8] in ['auction-only', 'block', 'indication-of-interest'] or data[row, 10] == 'market':
                continue

            else:
                # column 7 is event_type & column 13 is limit_price & column 17 is fill_quantity
                if data[row, 7] == 'Fill':
                    price_volume_dict[data[row, 13]] = round(price_volume_dict[data[row, 13]] - data[row, 17], 10)
                    for num in range(-num_of_best_prices, 0):
                        if np.isnan(data[row, num - num_of_best_prices]):
                            break
                        data[row, num] = price_volume_dict[data[row, num - num_of_best_prices]]

                # column 13 is limit_price & column 19 is remaining_quantity
                elif data[row, 7] == 'Cancel':
                    price_volume_dict[data[row, 13]] = round(price_volume_dict[data[row, 13]] - data[row, 19], 10)
                    for num in range(-num_of_best_prices, 0):
                        if np.isnan(data[row, num - num_of_best_prices]):
                            break
                        data[row, num] = price_volume_dict[data[row, num - num_of_best_prices]]

                else:
                    price_volume_dict[data[row, 13]] = round(price_volume_dict[data[row, 13]] + data[row, 19], 10)
                    for num in range(-num_of_best_prices, 0):
                        if np.isnan(data[row, num - num_of_best_prices]):
                            break
                        data[row, num] = price_volume_dict[data[row, num - num_of_best_prices]]

        result_data_set = pd.DataFrame(np.row_stack(data), columns=columns)
        result_data_set = result_data_set.astype(data_types)
        result_data_set = result_data_set.set_index('index')
        result_data_set.index.name = None

        return result_data_set

    @timer
    def get_best_volumes_split_by_account(self, data_set, num_of_best_prices):
        """
        Get the available volume of best prices split by account. Need to have best prices first.

        :param data_set: pd dataframe
        :param num_of_best_prices: int
        :return: pd dataframe
        """
        if not data_set['side'].duplicated()[1:].all():
            raise ValueError('Side can only be either buy or sell.')

        # initialize price & volume dict
        price_list = data_set['limit_price'].value_counts().keys().tolist()
        initial_volume = [{} for _ in range(len(price_list))]
        price_volume_dict = dict(zip(price_list, initial_volume))

        # initialize new best volume columns
        data_set = data_set.reset_index()
        original_columns = data_set.columns.tolist()
        best_account_volumes = ['best_account_volume_{}'.format(num) for num in range(1, num_of_best_prices + 1)]
        best_account_volumes_dict = {key_: '' for key_ in best_account_volumes}
        data_set = data_set.assign(**best_account_volumes_dict)
        data_set = data_set[original_columns + best_account_volumes]
        columns = data_set.columns
        data_types = data_set.dtypes
        data = data_set.to_numpy()

        for row in range(data.shape[0]):
            # column 8 is execution_options & column 10 is order_type
            if data[row, 8] in ['auction-only', 'block', 'indication-of-interest'] or data[row, 10] == 'market':
                continue

            else:
                # column 7 is event_type & column 1 is account_id & column 13 is limit_price & column 17 is fill_quantity
                if data[row, 7] == 'Fill':
                    price_volume_dict[data[row, 13]][data[row, 1]] = round(price_volume_dict[data[row, 13]][data[row, 1]] - data[row, 17], 10)
                    if price_volume_dict[data[row, 13]][data[row, 1]] == 0:
                        price_volume_dict[data[row, 13]].pop(data[row, 1])

                    for num in range(-num_of_best_prices, 0):
                        if np.isnan(data[row, num - num_of_best_prices]):
                            break
                        data[row, num] = str(price_volume_dict[data[row, num - num_of_best_prices]])

                # column 13 is limit_price & column 19 is remaining_quantity
                elif data[row, 7] == 'Cancel':
                    price_volume_dict[data[row, 13]][data[row, 1]] = round(price_volume_dict[data[row, 13]][data[row, 1]] - data[row, 19], 10)
                    if price_volume_dict[data[row, 13]][data[row, 1]] == 0:
                        price_volume_dict[data[row, 13]].pop(data[row, 1])

                    for num in range(-num_of_best_prices, 0):
                        if np.isnan(data[row, num - num_of_best_prices]):
                            break
                        data[row, num] = str(price_volume_dict[data[row, num - num_of_best_prices]])

                else:
                    if data[row, 1] in price_volume_dict[data[row, 13]]:
                        price_volume_dict[data[row, 13]][data[row, 1]] = round(price_volume_dict[data[row, 13]][data[row, 1]] + data[row, 19], 10)

                    else:
                        price_volume_dict[data[row, 13]].update({data[row, 1]: round(data[row, 19], 10)})

                    for num in range(-num_of_best_prices, 0):
                        if np.isnan(data[row, num - num_of_best_prices]):
                            break
                        data[row, num] = str(price_volume_dict[data[row, num - num_of_best_prices]])

        result_data_set = pd.DataFrame(np.row_stack(data), columns=columns)
        result_data_set = result_data_set.astype(data_types)
        result_data_set = result_data_set.set_index('index')
        result_data_set.index.name = None

        return result_data_set

    @timer
    def get_best_price_volume_on_the_other_side(self, data_set, num_of_best_prices):

        data_set = data_set.reset_index()
        original_columns = data_set.columns.tolist()
        best_prices = ['best_price_{}_other_side'.format(num) for num in range(1, num_of_best_prices + 1)]
        best_volumes = ['best_account_volume_{}_other_side'.format(num) for num in range(1, num_of_best_prices + 1)]
        best_prices_dict = {key_: np.nan for key_ in best_prices}
        best_volumes_dict = {key_: '' for key_ in best_volumes}
        data_set = data_set.assign(**best_prices_dict)
        data_set = data_set.assign(**best_volumes_dict)
        data_set = data_set[original_columns + best_prices + best_volumes]
        columns = data_set.columns
        data_types = data_set.dtypes
        data_array = data_set.to_numpy()

        for row in range(data_array.shape[0]):
            # column 8 is execution_options & column 10 is order_type
            if data_array[row, 8] in ['auction-only', 'block', 'indication-of-interest'] or data_array[row, 10] == 'market':
                continue

            for row_backward in range(row - 1, -1, -1):
                # column 11 is side & column -2 is best_price_1
                if data_array[row_backward, 11] != data_array[row, 11]:
                    data_array[row][-num_of_best_prices * 2: -num_of_best_prices] = data_array[row_backward][-num_of_best_prices * 4: -num_of_best_prices * 3]
                    data_array[row][-num_of_best_prices:] = data_array[row_backward][-num_of_best_prices * 3: -num_of_best_prices * 2]
                    break

        result_data_set = pd.DataFrame(np.row_stack(data_array), columns=columns)
        result_data_set = result_data_set.astype(data_types)
        result_data_set = result_data_set.set_index('index')
        result_data_set.index.name = None

        return result_data_set


# sample use case of this class
if __name__ == '__main__':
    calculation_module = DailyBestPrices()

    test_date = datetime.date(2019, 3, 10)
    test_instrument = 'LTCUSD'
    test_num_of_best_prices = 3

    test_data = calculation_module.get_data(test_instrument, test_date, test_date)
    test_result = calculation_module.calculation(test_data, test_instrument, test_num_of_best_prices)
