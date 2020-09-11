import datetime
import numpy as np
import plotly.offline as offplt
import plotly.graph_objs as go
import ast

from src.calculation.DailyBestPrices import DailyBestPrices
from src.utility.GeneralUtility import timer

offplt.offline.init_notebook_mode(connected=True)


@timer
def order_book_snap_shoot(instrument, evaluation_date, evaluation_time, num_of_best_prices=10, plot_out=False):

    hour = evaluation_time.hour
    minute = evaluation_time.minute
    second = evaluation_time.second
    micro_second = int(str(evaluation_time.microsecond).ljust(6, '0'))
    evaluation_time = datetime.time(hour, minute, second, micro_second)

    data_model = DailyBestPrices()
    base_data_set = data_model.get_data(instrument, evaluation_date, evaluation_date)[instrument]
    data_set = data_model.calculation(base_data_set, instrument, num_of_best_prices, split_best_volume_to_account=True)
    data_set = data_model.get_best_price_volume_on_the_other_side(data_set, num_of_best_prices)
    data_set = data_set.loc[data_set['event_type'] != 'Initial']
    data_set['date_time'] = data_set['date_time'].dt.time

    best_price = ['best_price_{}'.format(num) for num in range(1, num_of_best_prices + 1)]
    best_price_other_side = ['best_price_{}_other_side'.format(num) for num in range(1, num_of_best_prices + 1)]
    best_volumes = ['best_account_volume_{}'.format(num) for num in range(1, num_of_best_prices + 1)]
    best_volumes_other_side = ['best_account_volume_{}_other_side'.format(num) for num in range(1, num_of_best_prices + 1)]
    sub_data_set_columns = best_price + best_volumes + best_price_other_side + best_volumes_other_side + ['side', 'date_time']

    sub_data_set = data_set[sub_data_set_columns]
    result = sub_data_set.loc[sub_data_set['date_time'] == evaluation_time]

    if result.shape[0] > 1:
        result = result.head(1)

    best_volume_values = result[best_volumes + best_volumes_other_side].values.tolist()[0]
    accounts_list = [list(ast.literal_eval(x).keys()) for x in best_volume_values]
    unique_accounts_list = list(set([account for group in accounts_list for account in group]))

    data = []
    for account in unique_accounts_list:
        x_axis = []
        text_msg = []
        if result['side'].values[0] == 'sell':
            ask_prices = result[best_price].values.tolist()[0]
            ask_prices_str = ['Ask: ' + str(price) for price in ask_prices]
            bid_prices = result[best_price_other_side].values.tolist()[0]
            bid_prices.sort()
            bid_prices_str = ['Bid: ' + str(price) for price in bid_prices]
            y_axis = bid_prices_str + ask_prices_str

            for volume in reversed(best_volumes_other_side):
                x_axis.append(-1 if account in ast.literal_eval(result[volume].values[0]) else 0)
                text_msg.append(str(account) + ': ' + str(ast.literal_eval(result[volume].values[0])[account])
                                if account in ast.literal_eval(result[volume].values[0]) else 0)

            for volume in best_volumes:
                x_axis.append(1 if account in ast.literal_eval(result[volume].values[0]) else 0)
                text_msg.append(str(account) + ': ' + str(ast.literal_eval(result[volume].values[0])[account])
                                if account in ast.literal_eval(result[volume].values[0]) else 0)

        else:
            bid_prices = result[best_price].values.tolist()[0]
            bid_prices_str = ['Bid: ' + str(price) for price in bid_prices]
            bid_prices.sort()
            ask_prices = result[best_price_other_side].values.tolist()[0]
            ask_prices_str = ['Ask: ' + str(price) for price in ask_prices]
            y_axis = bid_prices_str + ask_prices_str

            for volume in reversed(best_volumes):
                x_axis.append(-1 if account in ast.literal_eval(result[volume].values[0]) else np.nan)
                text_msg.append(str(account) + ': ' + str(ast.literal_eval(result[volume].values[0])[account])
                                if account in ast.literal_eval(result[volume].values[0]) else np.nan)

            for volume in best_volumes_other_side:
                x_axis.append(1 if account in ast.literal_eval(result[volume].values[0]) else np.nan)
                text_msg.append(str(account) + ': ' + str(ast.literal_eval(result[volume].values[0])[account])
                                if account in ast.literal_eval(result[volume].values[0]) else np.nan)

        data.append(go.Bar(
            y=y_axis,
            x=x_axis,
            orientation='h',
            name=account,
            text=text_msg,
            textposition='auto',
            hoverinfo='skip'
        ))

    x_axis_tick_counts = []
    for bar in data:
        x_axis_tick_counts.append(np.abs(bar['x']))

    x_axis_tick_counts_sum = np.sum(x_axis_tick_counts, axis=0)
    x_axis_tick_counts_max = x_axis_tick_counts_sum.max()

    layout = go.Layout(title='{}: {} at {}'.format(instrument, evaluation_date, evaluation_time),
                       xaxis=go.XAxis(
                           range=[-x_axis_tick_counts_max, x_axis_tick_counts_max],
                           showticklabels=False
                       ),
                       barmode='stack',
                       bargap=0.1)

    fig = go.Figure(data=data, layout=layout)

    if plot_out:
        offplt.plot(fig, filename='{}_{}_{}.html'.format(instrument, evaluation_date, evaluation_time))

    return fig


# sample use case
if __name__ == '__main__':
    order_book_snap_shoot('LTCUSD', datetime.date(2019, 2, 24), datetime.time(23, 55, 39, 559), plot_out=True)
