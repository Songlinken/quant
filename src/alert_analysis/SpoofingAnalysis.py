import datetime
import numpy as np
import pandas as pd
import plotly.graph_objs as go
import plotly.offline as offplt

from src.data_models.DailyBestPricesSplitByAccountDataModel import DailyBestPricesSplitByAccountDataModel
from src.data_models.SmartsDataModel import SmartsDataModel
from src.utility.GeneralUtility import timer
from src.utility.MathUtility import sigmoid

offplt.offline.init_notebook_mode(connected=True)


@timer
def spoofing_analysis(account_id, instrument, evaluation_date, rank_level=10, scale_marker=False, plot_out=False):
    """
    Show details of the account action within evaluation date. The plot will show their order details.

    :param account_id: int
    :param instrument: str
    :param evaluation_date: date
    :param rank_level: int (max 10)
    :param scale_marker: bool
    :param plot_out: bool
    """
    base_data_set = SmartsDataModel().initialize(evaluation_date=evaluation_date, other_condition="symbol = '{}'".format(instrument)).evaluate()
    base_data_set['date_time'] = pd.to_datetime(base_data_set['event_date'].astype('str') + ' ' + base_data_set['event_time'].astype(str)) + base_data_set['event_millis']
    best_prices = DailyBestPricesSplitByAccountDataModel().initialize(evaluation_date=evaluation_date, other_condition="symbol = '{}'".format(instrument)).evaluate()
    best_prices = best_prices.drop(columns=['event_date', 'date_time', 'symbol', 'side'])
    result = base_data_set.merge(best_prices, how='left', on=['account_id', 'event_id', 'event_type'])
    result = get_bid_ask_pressure(result, rank_level)

    # pressure is defined as bid/ask
    result.loc[result['side'] == 'buy', 'pressure'] = result['current_side_pressure'] / result['opposite_side_pressure']
    result.loc[result['side'] == 'sell', 'pressure'] = result['opposite_side_pressure'] / result['current_side_pressure']
    result['market_price'] = result['fill_price'].ffill().fillna(method='bfill')
    result['mid_price'] = (result['best_price_1'] + result['best_price_1_other_side']) / 2.0

    result_with_place_time = get_place_time_of_order(result.loc[result['account_id'] == account_id])
    result_with_place_time = get_place_rank_of_order(result_with_place_time, rank_level)
    result_with_place_time['place_to_act'] = result_with_place_time['date_time'] - result_with_place_time['place_time']
    result_with_place_time['place_to_act'] = result_with_place_time['place_to_act'].dt.total_seconds()
    result_with_place_time['place_time'] = result_with_place_time['place_time'].dt.time
    result_with_place_time['cancel_text'] = ('place time: ' + result_with_place_time['place_time'].map(str) +
                                             '<br>place to cancel ' + result_with_place_time['place_to_act'].map(str) +
                                             '<br>cancel quantity: ' + result_with_place_time['remaining_quantity_crypto'].map(str) +
                                             '<br>execution options: ' + result_with_place_time['execution_options'].map(str) +
                                             '<br>place rank: ' + result_with_place_time['place_rank'].map(str) +
                                             '<br>cancel rank: ' + result_with_place_time['cancel_rank'].map(str) +
                                             '<br>pressure: ' + result_with_place_time['pressure'].map(str) +
                                             '<br>pressure change: ' + round(result_with_place_time['pressure_change'], 2).map(str) + '%'
                                             )
    result_with_place_time['fill_text'] = ('place time: ' + result_with_place_time['place_time'].map(str) +
                                           '<br>place to fill ' + result_with_place_time['place_to_act'].map(str) +
                                           '<br>fill quantity: ' + result_with_place_time['fill_quantity_crypto'].map(str) +
                                           '<br>execution options: ' + result_with_place_time['execution_options'].map(str) +
                                           '<br>place rank: ' + result_with_place_time['place_rank'].map(str) +
                                           '<br>cancel rank: ' + result_with_place_time['cancel_rank'].map(str) +
                                           '<br>pressure: ' + result_with_place_time['pressure'].map(str) +
                                           '<br>pressure change: ' + round(result_with_place_time['pressure_change'], 2).map(str) + '%'
                                           )

    # canceled ask of the account
    cancel_ask = result_with_place_time.loc[(result_with_place_time['event_type'] == 'Cancel') & (result_with_place_time['side'] == 'sell')]
    sub_cancel_ask = (cancel_ask[['date_time', 'event_id', 'limit_price', 'remaining_quantity_crypto', 'cancel_text']]
                      .rename({'limit_price': 'cancel_ask',
                               'remaining_quantity_crypto': 'cancel_ask_vol',
                               'cancel_text': 'cancel_ask_text'}, axis='columns'))

    # canceled bid of the account
    cancel_bid = result_with_place_time.loc[(result_with_place_time['event_type'] == 'Cancel') & (result_with_place_time['side'] == 'buy')]
    sub_cancel_bid = (cancel_bid[['date_time', 'event_id', 'limit_price', 'remaining_quantity_crypto', 'cancel_text']]
                      .rename({'limit_price': 'cancel_bid',
                               'remaining_quantity_crypto': 'cancel_bid_vol',
                               'cancel_text': 'cancel_bid_text'}, axis='columns'))

    # filled sell of the account
    fill_sell = result_with_place_time.loc[(result_with_place_time['event_type'] == 'Fill') & (result_with_place_time['side'] == 'sell')]
    sub_fill_sell = (fill_sell[['date_time', 'event_id', 'fill_price', 'fill_quantity_crypto', 'fill_text']]
                     .rename({'fill_price': 'fill_sell',
                              'fill_quantity_crypto': 'fill_sell_vol',
                              'fill_text': 'fill_sell_text'}, axis='columns'))

    # filled buy of the account
    fill_buy = result_with_place_time.loc[(result_with_place_time['event_type'] == 'Fill') & (result_with_place_time['side'] == 'buy')]
    sub_fill_buy = (fill_buy[['date_time', 'event_id', 'fill_price', 'fill_quantity_crypto', 'fill_text']]
                    .rename({'fill_price': 'fill_buy',
                             'fill_quantity_crypto': 'fill_buy_vol',
                             'fill_text': 'fill_buy_text'}, axis='columns'))

    # market best ask price
    best_ask = result.loc[(result['event_type'] != 'Initial') & (result['side'] == 'sell')]
    sub_best_ask = best_ask[['date_time', 'event_id', 'best_price_1']].rename({'best_price_1': 'best_ask'}, axis='columns')

    # market best bid price
    best_bid = result.loc[(result['event_type'] != 'Initial') & (result['side'] == 'buy')]
    sub_best_bid = best_bid[['date_time', 'event_id', 'best_price_1']].rename({'best_price_1': 'best_bid'}, axis='columns')

    # market price
    market_price = result.loc[result['event_type'] != 'Initial']
    sub_best_price = market_price[['date_time', 'event_id', 'market_price']]

    # market price
    mid_price = result.loc[result['event_type'] != 'Initial']
    sub_mid_price = mid_price[['date_time', 'event_id', 'mid_price']]

    # merge dfs
    market_df = pd.concat([sub_best_ask, sub_best_bid], sort=True).sort_values(['date_time', 'event_id']).ffill()
    final_df = (market_df.merge(sub_cancel_ask, on=['date_time', 'event_id'], how='left')
                .merge(sub_best_price, on=['date_time', 'event_id'], how='left')
                .merge(sub_mid_price, on=['date_time', 'event_id'], how='left')
                .merge(sub_cancel_bid, on=['date_time', 'event_id'], how='left')
                .merge(sub_fill_sell, on=['date_time', 'event_id'], how='left')
                .merge(sub_fill_buy, on=['date_time', 'event_id'], how='left'))

    if scale_marker:
        cancel_ask_scale = sigmoid(final_df['cancel_ask_vol'].to_numpy(), 0.001) * 20
        cancel_bid_scale = sigmoid(final_df['cancel_bid_vol'].to_numpy(), 0.001) * 20
        fill_buy_scale = sigmoid(final_df['fill_buy_vol'].to_numpy(), 0.001) * 20
        fill_sell_scale = sigmoid(final_df['fill_sell_vol'].to_numpy(), 0.001) * 20

        cancel_ask_scale = np.nan_to_num(cancel_ask_scale)
        cancel_bid_scale = np.nan_to_num(cancel_bid_scale)
        fill_buy_scale = np.nan_to_num(fill_buy_scale)
        fill_sell_scale = np.nan_to_num(fill_sell_scale)

    else:
        cancel_ask_scale = cancel_bid_scale = 8
        fill_buy_scale = fill_sell_scale = 12

    # plot graph
    sub_cancel_ask_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                      y=final_df['cancel_ask'],
                                      text=final_df['cancel_ask_text'],
                                      name='Cancelled Ask',
                                      mode='markers',
                                      marker={'symbol': 'circle',
                                              'size': cancel_ask_scale})

    sub_cancel_bid_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                      y=final_df['cancel_bid'],
                                      text=final_df['cancel_bid_text'],
                                      name='Cancelled Bid',
                                      mode='markers',
                                      marker={'symbol': 'square',
                                              'size': cancel_bid_scale})

    sub_best_ask_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                    y=final_df['best_ask'],
                                    name='Best Ask')

    sub_best_bid_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                    y=final_df['best_bid'],
                                    name='Best Bid')

    sub_market_price_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                        y=final_df['market_price'],
                                        name='Market Price')

    sub_mid_price_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                     y=final_df['mid_price'],
                                     name='Mid Price')

    sub_fill_sell_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                     y=final_df['fill_sell'],
                                     text=final_df['fill_sell_text'],
                                     name='Filled Sell',
                                     mode='markers',
                                     marker={'symbol': 'triangle-up',
                                             'size': fill_sell_scale,
                                             'color': '#ed47d7'})

    sub_fill_buy_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                    y=final_df['fill_buy'],
                                    text=final_df['fill_buy_text'],
                                    name='Filled Buy',
                                    mode='markers',
                                    marker={'symbol': 'x',
                                            'size': fill_buy_scale,
                                            'color': '#e6e600'})

    data = [sub_cancel_ask_plt, sub_cancel_bid_plt, sub_best_ask_plt, sub_best_bid_plt, sub_market_price_plt, sub_mid_price_plt, sub_fill_sell_plt, sub_fill_buy_plt]

    layout = go.Layout(title='{}: {} - {}'.format(account_id, instrument, evaluation_date))
    fig = go.Figure(data=data, layout=layout)

    if plot_out:
        offplt.plot(fig, filename='{}_{}_{}.html'.format(account_id, instrument, evaluation_date))

    return fig


@timer
def get_place_time_of_order(data_set):

    data_set = data_set.reset_index()
    data_set = data_set.assign(place_time=pd.NaT)

    columns = data_set.columns
    data_types = data_set.dtypes
    data_array = data_set.to_numpy()

    order_id_dict = {}
    for row in range(data_array.shape[0]):

        # column 7 is event_type & column 25 is date_time
        if data_array[row, 7] in ['Place', 'Initial']:
            order_id_dict.update({data_array[row, 9]: data_array[row, 25]})

        # column 9 is order_id
        else:
            data_array[row, -1] = order_id_dict[data_array[row, 9]]
            continue

    result_data_set = pd.DataFrame(np.row_stack(data_array), columns=columns)
    result_data_set = result_data_set.astype(data_types)
    result_data_set = result_data_set.set_index('index')
    result_data_set.index.name = None

    return result_data_set


@timer
def get_place_rank_of_order(data_set, number_of_best_prices):

    best_price_range = range(number_of_best_prices)

    data_set = data_set.reset_index()
    data_set = data_set.assign(place_rank='', fill_rank='', cancel_rank='')

    columns = data_set.columns
    data_types = data_set.dtypes
    data_array = data_set.to_numpy()

    order_id_dict = {}
    for row in range(data_array.shape[0]):
        # column 11 is side
        if data_array[row, 11] == 'buy':
            # column 7 is event_type
            if data_array[row, 7] in ['Place', 'Initial']:
                # column -3 is place_rank
                data_array[row, -3] = '> Rank {}'.format(number_of_best_prices)
                for best_price_rank in best_price_range:
                    # column 13 is limit_price & column 26+ is best_price+
                    if data_array[row, 13] >= data_array[row, 26 + best_price_rank]:
                        data_array[row, -3] = 'Rank {}'.format(best_price_rank + 1)
                        break
                # column 9 is order_id
                order_id_dict.update({data_array[row, 9]: data_array[row, -3]})

            elif data_array[row, 7] == 'Fill':
                data_array[row, -3] = order_id_dict[data_array[row, 9]]
                # column -2 is fill_rank
                data_array[row, -2] = '> Rank {}'.format(number_of_best_prices)
                for best_price_rank in best_price_range:
                    # column 16 is fill_price
                    if data_array[row, 16] >= data_array[row, 26 + best_price_rank]:
                        # column -2 is fill_rank
                        data_array[row, -2] = 'Rank {}'.format(best_price_rank + 1)
                        break

            # should be event_type == 'Cancel'
            else:
                data_array[row, -3] = order_id_dict[data_array[row, 9]]
                # column -1 is cancel_rank
                data_array[row, -1] = '> Rank {}'.format(number_of_best_prices)
                for best_price_rank in best_price_range:
                    if data_array[row, 13] >= data_array[row, 26 + best_price_rank]:
                        data_array[row, -1] = 'Rank {}'.format(best_price_rank + 1)
                        break

        # should be side = 'sell'
        else:
            if data_array[row, 7] in ['Place', 'Initial']:
                # column -3 is place_rank
                data_array[row, -3] = '> Rank {}'.format(number_of_best_prices)
                for best_price_rank in best_price_range:
                    # column 13 is limit_price & column 26+ is best_price+
                    if data_array[row, 13] <= data_array[row, 26 + best_price_rank]:
                        data_array[row, -3] = 'Rank {}'.format(best_price_rank + 1)
                        break
                # column 9 is order_id
                order_id_dict.update({data_array[row, 9]: data_array[row, -3]})

            elif data_array[row, 7] == 'Fill':
                data_array[row, -3] = order_id_dict[data_array[row, 9]]
                # column -2 is fill_rank
                data_array[row, -2] = '> Rank {}'.format(number_of_best_prices)
                for best_price_rank in best_price_range:
                    # column 16 is fill_price
                    if data_array[row, 16] <= data_array[row, 26 + best_price_rank]:
                        # column -2 is fill_rank
                        data_array[row, -2] = 'Rank {}'.format(best_price_rank + 1)
                        break

            # should be event_type == 'Cancel'
            else:
                data_array[row, -3] = order_id_dict[data_array[row, 9]]
                # column -1 is cancel_rank
                data_array[row, -1] = '> Rank {}'.format(number_of_best_prices)
                for best_price_rank in best_price_range:
                    if data_array[row, 13] <= data_array[row, 26 + best_price_rank]:
                        data_array[row, -1] = 'Rank {}'.format(best_price_rank + 1)
                        break

    result_data_set = pd.DataFrame(np.row_stack(data_array), columns=columns)
    result_data_set = result_data_set.astype(data_types)
    result_data_set = result_data_set.set_index('index')
    result_data_set.index.name = None

    return result_data_set


@timer
def get_bid_ask_pressure(data_set, rank_level):
    """
    Calculate pressure for both ask and bid sides. Need to load data of best prices and volumes for both sides first.

    :param data_set: pd dataframe
    :param rank_level: int (max 10)
    """
    data_set = data_set.reset_index()
    data_set = data_set.assign(current_side_pressure=np.nan, opposite_side_pressure=np.nan, pressure_change=np.nan)

    columns = data_set.columns
    data_types = data_set.dtypes
    data_array = data_set.to_numpy()

    for row in range(1, data_array.shape[0]):
        current_side_pressure = 0
        opposite_side_pressure = 0
        for rank in range(rank_level):
            # check if price of rank on current side is available
            if not np.isnan(data_array[row, rank - 3 - 4 * rank_level]):
                current_side_pressure += data_array[row, rank - 3 - 4 * rank_level] * sum(eval(data_array[row, rank - 3 - 3 * rank_level]).values())
                # column -3 is current_side_pressure
                data_array[row, -3] = current_side_pressure
            # check if price of rank on opposite side is available
            if not np.isnan(data_array[row, rank - 3 - 2 * rank_level]):
                opposite_side_pressure += data_array[row, rank - 3 - 2 * rank_level] * sum(eval(data_array[row, rank - 3 - rank_level]).values())
                # column -2 is opposite_side_pressure
                data_array[row, -2] = opposite_side_pressure
            # column 7 is event_type, column 13 is limit_price, column 14 is original_quantity_crypto
        if data_array[row, 7] == 'Place':
            for reverse_row in range(row - 1, 0, -1):
                # column 11 is side
                if data_array[reverse_row, 11] == data_array[row, 11]:
                    # column -1 is pressure_change
                    data_array[row, -1] = data_array[row, 13] * data_array[row, 14] / data_array[reverse_row, -3] * 100
                    break
        elif data_array[row, 7] == 'Cancel':
            for reverse_row in range(row - 1, 0, -1):
                if data_array[reverse_row, 11] == data_array[row, 11]:
                    data_array[row, -1] = -data_array[row, 13] * data_array[row, 14] / data_array[reverse_row, -3] * 100
                    break
        elif data_array[row, 7] == 'Fill':
            for reverse_row in range(row - 1, 0, -1):
                if data_array[reverse_row, 11] == data_array[row, 11]:
                    # column 16 is fill_price, column 17 is fill_price
                    data_array[row, -1] = -data_array[row, 16] * data_array[row, 17] / data_array[reverse_row, -3] * 100
                    break

    result_data_set = pd.DataFrame(np.row_stack(data_array), columns=columns)
    result_data_set = result_data_set.astype(data_types)
    result_data_set = result_data_set.set_index('index')
    result_data_set.index.name = None

    return result_data_set


# sample use case
if __name__ == '__main__':
    spoofing_analysis(1595, 'BTCUSD', datetime.date(2019, 11, 12), plot_out=True)
