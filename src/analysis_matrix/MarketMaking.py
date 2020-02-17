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
def market_making(instrument, evaluation_date,
                  mid_price_method='market', initial_q=0, sigma=2, gamma=0.1, liquidity=1.5, time_step=datetime.timedelta(seconds=5),
                  scale_marker=False, plot_out=False):
    """
    Show details of the account action within evaluation date. The plot will show their order details.
    Only works within a day.

    :param instrument: str
    :param evaluation_date: date
    :param mid_price_method: str (central or market)
    :param initial_q: int or float (inventory of frozen inventory model)
    :param sigma: float (instrument volatility)
    :param gamma: float (risk aversion)
    :param liquidity: float (measure of the liquidity of the market)
    :param time_step: timedelta of seconds (alive time for orders)
    :param scale_marker: bool
    :param plot_out: bool
    """
    if mid_price_method not in ['central', 'market']:
        raise ValueError('mid_price_method must be "central" or "market".')

    for parameter in [initial_q, sigma, gamma, liquidity]:
        if not isinstance(parameter, (float, int)):
            raise ValueError('{} is not in right data type.'.format(parameter))

    if not isinstance(time_step, datetime.timedelta):
        raise ValueError('time_step has to be datetime.timedelta with seconds.')

    base_data_set = SmartsDataModel().initialize(evaluation_date=evaluation_date, other_condition="symbol = '{}'".format(instrument)).evaluate()
    base_data_set['date_time'] = pd.to_datetime(base_data_set['event_date'].astype('str') + ' ' + base_data_set['event_time'].astype(str)) + base_data_set['event_millis']
    best_prices = DailyBestPricesSplitByAccountDataModel().initialize(evaluation_date=evaluation_date, other_condition="symbol = '{}'".format(instrument)).evaluate()
    best_prices = best_prices.drop(columns=['event_date', 'date_time', 'symbol', 'side'])
    result = base_data_set.merge(best_prices, how='left', on=['account_id', 'event_id', 'event_type'])
    result['mid_price'] = (result['best_price_1'] + result['best_price_1_other_side']) / 2.0
    result['market_price'] = result['fill_price'].ffill().fillna(method='bfill')

    result_with_optimal_quotes = frozen_inventory_approximation(result, mid_price_method, initial_q, sigma, gamma, liquidity, time_step)

    # market best ask price
    best_ask = result_with_optimal_quotes.loc[(result_with_optimal_quotes['event_type'] != 'Initial') & (result_with_optimal_quotes['side'] == 'sell')]
    sub_best_ask = best_ask[['date_time', 'event_id', 'best_price_1']].rename({'best_price_1': 'best_ask'}, axis='columns')

    # market best bid price
    best_bid = result_with_optimal_quotes.loc[(result_with_optimal_quotes['event_type'] != 'Initial') & (result_with_optimal_quotes['side'] == 'buy')]
    sub_best_bid = best_bid[['date_time', 'event_id', 'best_price_1']].rename({'best_price_1': 'best_bid'}, axis='columns')

    # filled buy of simulation
    fill_sell = result_with_optimal_quotes.loc[(~result_with_optimal_quotes['simulated_fill_price'].isna()) & (result_with_optimal_quotes['side'] == 'buy')]
    sub_fill_sell = (fill_sell[['date_time', 'event_id', 'simulated_fill_price', 'simulated_fill_quantity']]
                     .rename({'simulated_fill_price': 'fill_sell',
                              'simulated_fill_quantity': 'fill_sell_vol'}, axis='columns'))
    # filled sell of simulation
    fill_buy = result_with_optimal_quotes.loc[(~result_with_optimal_quotes['simulated_fill_price'].isna()) & (result_with_optimal_quotes['side'] == 'sell')]
    sub_fill_buy = (fill_buy[['date_time', 'event_id', 'simulated_fill_price', 'simulated_fill_quantity']]
                    .rename({'simulated_fill_price': 'fill_buy',
                             'simulated_fill_quantity': 'fill_buy_vol'}, axis='columns'))

    # market price & optimal quotes
    market_optimal_price = result_with_optimal_quotes.loc[result['event_type'] != 'Initial']
    sub_market_optimal_price = market_optimal_price[['date_time', 'event_id', 'market_price', 'optimal_bid', 'optimal_ask']]

    # merge dfs
    market_df = pd.concat([sub_best_ask, sub_best_bid], sort=True).sort_values(['date_time', 'event_id']).ffill()
    final_df = (market_df.merge(sub_market_optimal_price, on=['date_time', 'event_id'], how='left')
                .merge(sub_fill_sell, on=['date_time', 'event_id'], how='left')
                .merge(sub_fill_buy, on=['date_time', 'event_id'], how='left'))

    if scale_marker:
        optimal_bid_scale = sigmoid(final_df['optimal_bid'].to_numpy(), 0.001) * 20
        optimal_ask_scale = sigmoid(final_df['optimal_ask'].to_numpy(), 0.001) * 20
        fill_buy_scale = sigmoid(final_df['fill_buy_vol'].to_numpy(), 0.001) * 20
        fill_sell_scale = sigmoid(final_df['fill_sell_vol'].to_numpy(), 0.001) * 20

        optimal_bid_scale = np.nan_to_num(optimal_bid_scale)
        optimal_ask_scale = np.nan_to_num(optimal_ask_scale)
        fill_buy_scale = np.nan_to_num(fill_buy_scale)
        fill_sell_scale = np.nan_to_num(fill_sell_scale)

    else:
        optimal_bid_scale = optimal_ask_scale = 8
        fill_buy_scale = fill_sell_scale = 12

    # plot graph
    sub_best_ask_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                    y=final_df['best_ask'],
                                    name='Best Ask')

    sub_best_bid_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                    y=final_df['best_bid'],
                                    name='Best Bid')

    sub_market_price_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                        y=final_df['market_price'],
                                        name='Market Price')

    sub_optimal_ask_scale_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                             y=final_df['optimal_ask'],
                                             name='Optimal Ask',
                                             mode='markers',
                                             marker={'symbol': 'circle',
                                                     'size': optimal_ask_scale})

    sub_optimal_bid_scale_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                             y=final_df['optimal_bid'],
                                             name='Optimal Bid',
                                             mode='markers',
                                             marker={'symbol': 'square',
                                                     'size': optimal_bid_scale})

    sub_fill_sell_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                     y=final_df['fill_sell'],
                                     name='Simulated Filled Sell',
                                     mode='markers',
                                     marker={'symbol': 'triangle-up',
                                             'size': fill_sell_scale,
                                             'color': '#ed47d7'})

    sub_fill_buy_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                    y=final_df['fill_buy'],
                                    name='Simulated Filled Buy',
                                    mode='markers',
                                    marker={'symbol': 'x',
                                            'size': fill_buy_scale,
                                            'color': '#e6e600'})

    data = [sub_optimal_ask_scale_plt, sub_optimal_bid_scale_plt, sub_best_ask_plt, sub_best_bid_plt, sub_market_price_plt, sub_fill_sell_plt, sub_fill_buy_plt]

    layout = go.Layout(title='Market Making Simulation: {} - {}'.format(instrument, evaluation_date))
    fig = go.Figure(data=data, layout=layout)

    if plot_out:
        offplt.plot(fig, filename='market_making_simulation_{}_{}.html'.format(instrument, evaluation_date))

    return fig


@timer
def frozen_inventory_approximation(data_set, mid_price_method, initial_q, sigma, gamma, liquidity, time_step):
    """
    Frozen inventory approximation is a method derived by Stoikov, which is used to model optimal bid and ask.

    :param mid_price_method: str (central or market)
    :param initial_q: float (inventory of frozen inventory model)
    :param sigma: float (instrument volatility)
    :param gamma: float (risk aversion)
    :param liquidity: float (measure of the liquidity of the market)
    :param time_step: timedelta of seconds (alive time for orders)
    """
    data_set = data_set.reset_index()
    data_set = data_set.assign(optimal_bid=np.nan, optimal_ask=np.nan, simulated_fill_price=np.nan, simulated_fill_quantity=np.nan)

    columns = data_set.columns
    data_types = data_set.dtypes
    data_array = data_set.to_numpy()

    row_ahead = 0
    for row in range(data_array.shape[0] - 1):
        if row <= row_ahead:
            continue
        # column 7 is event_type
        if data_array[row, 7] == 'Initial':
            continue
        # column -5 is market_price
        if mid_price_method == 'market':
            indifference_price = data_array[row, -5] - initial_q * gamma * (sigma ** 2) * time_step.total_seconds()
        # column -6 is mid_price
        else:
            indifference_price = data_array[row, -6] - initial_q * gamma * (sigma ** 2) * time_step.total_seconds()
        # column -3 is optimal_ask, column -4 is optimal_bid
        data_array[row, -4] = indifference_price - (1 / gamma) * np.log(1 + gamma / liquidity)
        data_array[row, -3] = indifference_price + (1 / gamma) * np.log(1 + gamma / liquidity)

        for row_two in range(row + 1, data_array.shape[0]):
            # column 25 is date_time
            if data_array[row_two, 25] - data_array[row, 25] > time_step:
                break
            # column 7 is event_type
            if data_array[row_two, 7] == 'Fill':
                # column 11 is side, column 16 is fill_price
                if data_array[row_two, 11] == 'buy' and data_array[row_two, 16] >= data_array[row, -3]:
                    # column 17 is fill_quantity_crypto, column -2 is simulated_fill_price, column -1 is simulated_fill_quantity
                    data_array[row_two, -2] = data_array[row_two, 16]
                    data_array[row_two, -1] = data_array[row_two, 17]
                    initial_q = initial_q - data_array[row_two, 17]

                elif data_array[row_two, 11] == 'sell' and data_array[row_two, 16] <= data_array[row, -2]:
                    data_array[row_two, -2] = data_array[row_two, 16]
                    data_array[row_two, -1] = data_array[row_two, 17]
                    initial_q = initial_q + data_array[row_two, 17]

            row_ahead = row_two

    result_data_set = pd.DataFrame(np.row_stack(data_array), columns=columns)
    result_data_set = result_data_set.astype(data_types)
    result_data_set = result_data_set.set_index('index')
    result_data_set.index.name = None

    return result_data_set


if __name__ == '__main__':
    instruments = market_making('BTCUSD', datetime.date(2019, 5, 26), plot_out=True)
