import datetime
import numpy as np
import pandas as pd
import plotly.graph_objs as go
import plotly.offline as offplt

from src.alert_analysis.AnalysisNode import AnalysisNode
from src.data_models.DailyBestPricesDataModel import DailyBestPricesDataModel
from src.data_models.SmartsDataModel import SmartsDataModel
from src.utility.GeneralUtility import timer


offplt.offline.init_notebook_mode(connected=True)


class MultiOrderSpoofingAnalysis(AnalysisNode):
    """
    Analysis of multi-order spoofing alerts from SMARTS (code: 4032).
    """
    @timer
    def get_alert(self, account_id, instrument, evaluation_date):

        alerts_df = pd.read_csv('~/Downloads/4032_{}_Broker_{}_{}.csv'.format(instrument, account_id, str(evaluation_date).replace('-', '')))

        # clear data
        alerts_df['Amend/delete time Spoofing Side'] = (str(evaluation_date) + alerts_df['Amend/delete time Spoofing Side']).astype('datetime64[ns]')
        alerts_df['Trade time Genuine Side'] = (str(evaluation_date) + alerts_df['Trade time Genuine Side']).astype('datetime64[ns]')

        return alerts_df

    @timer
    def activities_with_alert_time(self, account_id, instrument, evaluation_date, plot_out=True):
        base_data_set = SmartsDataModel().initialize(evaluation_date=evaluation_date, other_condition="symbol = '{}'".format(instrument)).evaluate()
        base_data_set['date_time'] = pd.to_datetime(base_data_set['event_date'].astype('str') + ' ' + base_data_set['event_time'].astype(str)) + base_data_set['event_millis']
        best_prices = DailyBestPricesDataModel().initialize(evaluation_date=evaluation_date, other_condition="symbol = '{}'".format(instrument)).evaluate()
        base_data_set = base_data_set.merge(best_prices[['account_id', 'event_id', 'event_type', 'best_price_1']], how='left', on=['account_id', 'event_id', 'event_type'])
        alerts_df = self.get_alert(account_id, instrument, evaluation_date)

        # get max & min time for each alert
        alerts_df.loc[alerts_df['Amend/delete time Spoofing Side'].isna(), 'Amend/delete time Spoofing Side'] = alerts_df['Trade time Genuine Side']

        alerts_time_range = alerts_df.groupby('Spoofing Event Nr')['Amend/delete time Spoofing Side'].agg({'Amend/delete time Spoofing Side': [np.max, np.min]})
        alerts_time_range['Amend/delete time Spoofing Side', 'amax'] = alerts_time_range['Amend/delete time Spoofing Side', 'amax'].dt.time
        alerts_time_range['Amend/delete time Spoofing Side', 'amin'] = alerts_time_range['Amend/delete time Spoofing Side', 'amin'].dt.time
        alert_time_ranges = alerts_time_range.values.tolist()

        sub_data_set = base_data_set.loc[base_data_set['event_date'] == evaluation_date]

        alert_time_sub_data_set_list = []
        for range_ in alert_time_ranges:
            alert_time_sub_data_set_list.append(sub_data_set.loc[(sub_data_set['date_time'].dt.time <= max(range_)) & (sub_data_set['date_time'].dt.time >= min(range_))])
        result = pd.concat(alert_time_sub_data_set_list).drop_duplicates()

        # annotation of points on plot
        result.loc[result['event_type'] == 'Cancel', 'cancel_text'] = ('cancel quantity: ' +
                                                                       result.loc[result['event_type'] == 'Cancel', 'remaining_quantity_crypto'].map(str) +
                                                                       '<br>execution options: ' +
                                                                       result.loc[result['event_type'] == 'Cancel', 'execution_options'])

        result.loc[result['event_type'] == 'Fill', 'fill_text'] = ('fill quantity: ' +
                                                                   result.loc[result['event_type'] == 'Fill', 'fill_quantity_crypto'].map(str) +
                                                                   '<br>execution options: ' +
                                                                   result.loc[result['event_type'] == 'Fill', 'execution_options'])

        # canceled ask of the account
        alert_account_cancel_ask = result.loc[(result['account_id'] == account_id) &
                                              (result['side'] == 'sell') &
                                              (result['event_type'] == 'Cancel'),
                                              ['date_time', 'event_id', 'limit_price', 'remaining_quantity_crypto', 'cancel_text']]

        sub_alert_account_cancel_ask = alert_account_cancel_ask.rename({'limit_price': 'cancel_ask',
                                                                        'remaining_quantity_crypto': 'cancel_ask_vol',
                                                                        'cancel_text': 'cancel_ask_text'}, axis='columns')

        # canceled bid of the account
        alert_account_cancel_bid = result.loc[(result['account_id'] == account_id) &
                                              (result['side'] == 'buy') &
                                              (result['event_type'] == 'Cancel'),
                                              ['date_time', 'event_id', 'limit_price', 'remaining_quantity_crypto', 'cancel_text']]

        sub_alert_account_cancel_bid = alert_account_cancel_bid.rename({'limit_price': 'cancel_bid',
                                                                        'remaining_quantity_crypto': 'cancel_bid_vol',
                                                                        'cancel_text': 'cancel_bid_text'}, axis='columns')

        # filled sell of the account
        alert_account_fill_sell = result.loc[(result['account_id'] == account_id) &
                                             (result['side'] == 'sell') &
                                             (result['event_type'] == 'Fill'),
                                             ['date_time', 'event_id', 'fill_price', 'fill_quantity_crypto', 'fill_text']]

        sub_alert_account_fill_sell = alert_account_fill_sell.rename({'fill_price': 'fill_sell',
                                                                      'fill_quantity_crypto': 'fill_sell_vol',
                                                                      'fill_text': 'fill_sell_text'}, axis='columns')

        # filled buy of the account
        alert_account_fill_buy = result.loc[(result['account_id'] == account_id) &
                                            (result['side'] == 'buy') &
                                            (result['event_type'] == 'Fill'),
                                            ['date_time', 'event_id', 'fill_price', 'fill_quantity_crypto', 'fill_text']]

        sub_alert_account_fill_buy = alert_account_fill_buy.rename({'fill_price': 'fill_buy',
                                                                    'fill_quantity_crypto': 'fill_buy_vol',
                                                                    'fill_text': 'fill_buy_text'}, axis='columns')

        # filled sell of other accounts
        other_account_fill_sell = result.loc[(result['account_id'] != account_id) &
                                             (result['side'] == 'sell') &
                                             (result['event_type'] == 'Fill'),
                                             ['date_time', 'event_id', 'fill_price', 'fill_quantity_crypto', 'fill_text']]

        sub_other_account_fill_sell = other_account_fill_sell.rename({'fill_price': 'fill_sell_other',
                                                                      'fill_quantity_crypto': 'fill_sell_vol_other',
                                                                      'fill_text': 'fill_sell_text_other'}, axis='columns')

        # filled buy of other accounts
        other_account_fill_buy = result.loc[(result['account_id'] != account_id) &
                                            (result['side'] == 'buy') &
                                            (result['event_type'] == 'Fill'),
                                            ['date_time', 'event_id', 'fill_price', 'fill_quantity_crypto', 'fill_text']]

        sub_other_account_fill_buy = other_account_fill_buy.rename({'fill_price': 'fill_buy_other',
                                                                    'fill_quantity_crypto': 'fill_buy_vol_other',
                                                                    'fill_text': 'fill_buy_text_other'}, axis='columns')

        # market best ask price
        best_ask = result.loc[result['side'] == 'sell']
        sub_best_ask = best_ask[['date_time', 'event_id', 'best_price_1']].rename({'best_price_1': 'best_ask'}, axis='columns')

        # market best bid price
        best_bid = result.loc[result['side'] == 'buy']
        sub_best_bid = best_bid[['date_time', 'event_id', 'best_price_1']].rename({'best_price_1': 'best_bid'}, axis='columns')

        # merge dfs
        market_df = pd.concat([sub_best_ask, sub_best_bid], sort=True).sort_values(['date_time', 'event_id']).ffill()

        final_df = (market_df.merge(sub_alert_account_cancel_ask, on=['date_time', 'event_id'], how='left')
                    .merge(sub_alert_account_cancel_bid, on=['date_time', 'event_id'], how='left')
                    .merge(sub_alert_account_fill_sell, on=['date_time', 'event_id'], how='left')
                    .merge(sub_alert_account_fill_buy, on=['date_time', 'event_id'], how='left')
                    .merge(sub_other_account_fill_sell, on=['date_time', 'event_id'], how='left')
                    .merge(sub_other_account_fill_buy, on=['date_time', 'event_id'], how='left'))

        # plot graph
        marker_size = 15

        sub_cancel_ask_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                          y=final_df['cancel_ask'],
                                          text=final_df['cancel_ask_text'],
                                          name='{} Cancelled Ask'.format(account_id),
                                          mode='markers',
                                          marker={'symbol': 'circle',
                                                  'size': marker_size})

        sub_cancel_bid_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                          y=final_df['cancel_bid'],
                                          text=final_df['cancel_bid_text'],
                                          name='{} Cancelled Bid'.format(account_id),
                                          mode='markers',
                                          marker={'symbol': 'square',
                                                  'size': marker_size})

        sub_best_ask_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                        y=final_df['best_ask'],
                                        name='Best Ask')

        sub_best_bid_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                        y=final_df['best_bid'],
                                        name='Best Bid')

        sub_fill_sell_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                         y=final_df['fill_sell'],
                                         text=final_df['fill_sell_text'],
                                         name='{} Filled Sell'.format(account_id),
                                         mode='markers',
                                         marker={'symbol': 'triangle-up',
                                                 'color': 'cyan',
                                                 'size': marker_size})

        sub_fill_buy_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                        y=final_df['fill_buy'],
                                        text=final_df['fill_buy_text'],
                                        name='{} Filled Buy'.format(account_id),
                                        mode='markers',
                                        marker={'symbol': 'x',
                                                'color': 'yellow',
                                                'size': marker_size})

        sub_fill_sell_other_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                               y=final_df['fill_sell_other'],
                                               text=final_df['fill_sell_text_other'],
                                               name='Other Filled Sell',
                                               mode='markers',
                                               marker={'symbol': 'triangle-up',
                                                       'color': 'brown',
                                                       'size': marker_size})

        sub_fill_other_buy_plt = go.Scattergl(x=final_df['date_time'].dt.time,
                                              y=final_df['fill_buy_other'],
                                              text=final_df['fill_buy_text_other'],
                                              name='Other Filled Buy'.format(account_id),
                                              mode='markers',
                                              marker={'symbol': 'x',
                                                      'color': 'grey',
                                                      'size': marker_size})

        data = [sub_cancel_ask_plt, sub_cancel_bid_plt, sub_best_ask_plt, sub_best_bid_plt, sub_fill_sell_plt, sub_fill_buy_plt,
                sub_fill_sell_other_plt, sub_fill_other_buy_plt]

        layout = go.Layout(title='{}: {} - {} (Alert Level)'.format(account_id, instrument, evaluation_date))
        fig = go.Figure(data=data, layout=layout)

        if plot_out:
            offplt.plot(fig, filename='{}_{}_{}_alert_level.html'.format(account_id, instrument, evaluation_date))

        return fig


# sample use case of this class
if __name__ == '__main__':
    MultiOrderSpoofingAnalysis().activities_with_alert_time(772544, 'BTCUSD', datetime.date(2019, 5, 19))
