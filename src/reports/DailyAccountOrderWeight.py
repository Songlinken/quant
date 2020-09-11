import datetime
import numpy as np
import pandas as pd
import plotly.offline as offplt
import plotly.graph_objs as go

from src.data_models.SmartsDataModel import SmartsDataModel
from src.data_models.DailyBestPricesDataModel import DailyBestPricesDataModel
from src.data_models.SmartsCsvDataModel import SmartsCsvDataModel
from src.utility.DataModelUtility import execute_query_data_frame
from src.utility.GeneralUtility import timer
from src.utility.MathUtility import sigmoid

offplt.offline.init_notebook_mode(connected=True)


@timer
def show_account_daily_action(account_id, instrument, evaluation_date, plot_out=False):
    """
    Show details of the account action within evaluation date. The plot will show their order details.

    :param account_id: int
    :param instrument: str
    :param evaluation_date: date
    :param scale_marker: bool
    """
    base_data_set = SmartsDataModel().initialize(evaluation_date=evaluation_date, other_condition="symbol = '{}'".format(instrument)).evaluate()
    best_prices = DailyBestPricesDataModel().initialize(evaluation_date=evaluation_date, other_condition="symbol = '{}'".format(instrument)).evaluate()
    result = base_data_set.merge(best_prices[['account_id', 'event_id', 'event_type', 'best_price_1']], how='left', on=['account_id', 'event_id', 'event_type'])

    result_with_order_weight = get_order_weight_of_order(result.loc[result['account_id'] == account_id][['event_type', 'order_id', 'side', 'remaining_quantity', 'fill_quantity', 'date_time', 'event_id']])

    # market best ask price (exclude_extreme_points)
    best_ask = result.loc[(result['event_type'] != 'Initial') & (result['side'] == 'sell')]
    sub_best_ask = best_ask[['date_time', 'event_id', 'best_price_1']].rename({'best_price_1': 'best_ask'}, axis='columns')

    # market best bid price (exclude_extreme_points)
    best_bid = result.loc[(result['event_type'] != 'Initial') & (result['side'] == 'buy')]
    sub_best_bid = best_bid[['date_time', 'event_id', 'best_price_1']].rename({'best_price_1': 'best_bid'}, axis='columns')

    # order_weights at all events
    order_weights = result_with_order_weight[['date_time', 'event_id', 'order_weight']]
    order_weights.drop_duplicates(subset='date_time', keep='last', inplace=True)
    order_weights = order_weights.loc[order_weights.date_time >= pd.Timestamp(evaluation_date)]

    result_with_order_weight.order_weight = result_with_order_weight.order_weight.shift(periods=1)

    order_fills = result_with_order_weight.loc[result_with_order_weight.fill_quantity.notnull()][['date_time', 'event_id', 'fill_quantity', 'side', 'order_weight']]
    import ipdb; ipdb.set_trace()
    order_fills['fill_quantity_dir'] = order_fills.apply(lambda x: x.fill_quantity if x.side == 'buy' else -x.fill_quantity, axis=1)
    order_fills['weight_fill_ratio'] = order_fills.order_weight/order_fills.fill_quantity_dir
    order_fills = order_fills.drop(columns=['side'])

    subset = order_fills.loc[(order_fills.weight_fill_ratio > -200) & (order_fills.weight_fill_ratio < 200)]
    data = [go.Histogram(cumulative=dict(enabled=True), x=subset.weight_fill_ratio, y=subset.fill_quantity, name='Weight Fill Ratio', histfunc='sum', nbinsx=2000)]
    data2 = [go.Histogram(x=subset.weight_fill_ratio, y=subset.fill_quantity, name='Weight Fill Ratio', histfunc='sum', nbinsx=2000)]
    layout = go.Layout(title='Weight - Fill Ratio {}: {} - {}'.format(account_id, instrument, evaluation_date))
    fig = go.Figure(data=data, layout=layout)
    fig2 = go.Figure(data=data2, layout=layout)
    offplt.plot(fig, filename='Cumulative WFR_{}_{}_{}.html'.format(account_id, instrument, evaluation_date))
    offplt.plot(fig2, filename='WFR_{}_{}_{}.html'.format(account_id, instrument, evaluation_date))


    # merge dfs
    final_df = pd.concat([sub_best_ask, sub_best_bid], sort=True).sort_values(['date_time', 'event_id']).ffill()

    # plot graph
    sub_best_ask_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                    y=final_df['best_ask'],
                                    name='Best Ask')

    sub_best_bid_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                    y=final_df['best_bid'],
                                    name='Best Bid')

    # import ipdb; ipdb.set_trace()
    sub_order_weight_plt = go.Scatter(x=order_weights['date_time'].dt.time,
                                        y=order_weights['order_weight'],
                                        mode='lines',
                                        fill='tonexty',
                                        yaxis='y2',
                                        name='Order Weight')

    sub_fill_value_plt = go.Bar(x=order_fills['date_time'].dt.time,
                                      y=order_fills['fill_quantity_dir'],
                                      # mode='markers',
                                      name='Net Fill Value',
                                      yaxis='y3')

    data = [sub_best_ask_plt, sub_best_bid_plt, sub_order_weight_plt, sub_fill_value_plt]
    # data = [sub_order_weight_plt]

    layout = go.Layout(title='{}: {} - {}'.format(account_id, instrument, evaluation_date),
                       yaxis=dict(
                           title='Price',
                           side='right',
                           range=[final_df.best_bid.min(), final_df.best_ask.max()]
                       ),
                       yaxis2=dict(
                           title='Weight Volume',
                           overlaying='y',
                           side='left',
                           anchor='x',
                           range=[order_weights.order_weight.min(), order_weights.order_weight.max()]
                       ),
                       yaxis3=dict(
                           title='Fill Volume',
                           overlaying='y',
                           side='left',
                           anchor='free',
                           position=0.05,
                           layer='above traces',
                           range=[order_fills.fill_quantity.min(), order_fills.fill_quantity.max()]
                       ),
                       margin=dict(
                           r=150,
                           b=120
                       )
                       )
    fig = go.Figure(data=data, layout=layout)

    if plot_out:
        offplt.plot(fig, filename='{}_{}_{}.html'.format(account_id, instrument, evaluation_date))

    return fig


@timer
def get_order_weight_of_order(data_set):
    data_set = data_set.reset_index()
    data_set = data_set.assign(order_weight=np.NaN)

    columns = data_set.columns
    data_types = data_set.dtypes
    data_array = data_set.to_numpy()

    order_id_dict = pd.DataFrame()

    for row in range(data_array.shape[0]):
        # row 7 is event_type
        side = data_array[row, 3]
        quant_val = data_array[row, 4]
        net_val = quant_val if side == 'buy' else -quant_val

        if data_array[row, 1] in ['Initial', 'Place']:
            order_row = pd.DataFrame({'order_id': [data_array[row, 2]], 'quantity': [net_val], 'side': [side]}).set_index('order_id')
            order_id_dict = order_id_dict.append(order_row)
        elif (data_array[row, 1] == 'Fill') & (net_val != 0):
            order_id_dict.at[data_array[row, 2], 'quantity'] = net_val
        else:
            try:
                order_id_dict = order_id_dict.drop(data_array[row, 2])
            except KeyError:
                print('order already removed')

        data_array[row, -1] = order_id_dict.quantity.sum()

    result_data_set = pd.DataFrame(np.row_stack(data_array), columns=columns)
    result_data_set = result_data_set.astype(data_types)
    result_data_set = result_data_set.set_index('index')
    result_data_set.index.name = None

    return result_data_set


# sample use case
if __name__ == '__main__':
    show_account_daily_action(976037, 'ZECLTC', datetime.date(2019, 5, 25), plot_out=True)
