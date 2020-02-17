def commodity_channel_index(data_set, number_of_periods=20):
    """
    Calculate CCI. Please sort data frame in ascending time order.
    :param data_set: pd dataframe
    :param number_of_periods: int
    :return: pd dataframe
    """
    data_set['typical_price'] = (data_set['high_price'] + data_set['low_price'] + data_set['close_price']) / 3.0
    data_set['moving_average'] = data_set['typical_price'].rolling(number_of_periods).mean()
    data_set['mean_deviation'] = data_set['typical_price'].rolling(number_of_periods).std()
    data_set['cci'] = (data_set['typical_price'] - data_set['moving_average']) / (0.015 * data_set['mean_deviation'])

    return data_set


def ease_of_movement(data_set, scale=1):
    """
    Calculate EVM. Please sort data frame in ascending time order.
    :param data_set: pd dataframe
    :param scale: int (Scale depends on the average daily volume of the stock.
                       The more heavily traded the stock, the higher the scale should be to keep the indicator value in single or double digits.)
    :return: pd dataframe
    """

    data_set['previous_high'] = data_set['high_price'].shift(1)
    data_set['previous_low'] = data_set['low_price'].shift(1)
    data_set['distance'] = (data_set['high_price'] + data_set['low_price']) / 2 - (data_set['previous_high'] - data_set['previous_low']) / 2
    data_set['box_ratio'] = data_set['volume_from'] / scale / (data_set['high_price'] - data_set['low_price'])
    data_set['evm'] = data_set['distance'] / data_set['box_ratio']

    return data_set


def moving_average(data_set, period=5):
    """
    Calculate MV. Please sort data frame in ascending time order.
    :param data_set: pd dataframe
    :param period: int (Time window to calculate moving average)
    :return: pd dataframe
    """
    data_set['mv_{}'.format(period)] = data_set['close_price'].rolling(window=period).mean()

    return data_set
