import datetime
import pandas as pd
import plotly.graph_objs as go
import plotly.offline as offplt

from src.calculation.CalculationNode import CalculationNode
from src.data_models.AccountDerivedMetaDataModel import AccountDerivedMetaDataModel
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.data_models.OrderFillEventDataModel import OrderFillEventDataModel
from src.utility.GeneralUtility import timer

offplt.offline.init_notebook_mode(connected=True)


class MarketMakerAndLiquidity(CalculationNode):
    """
    This class designed to calculate market maker matrix as well as liquidity of market.
    """
    def get_data(self, instrument, evaluation_date, begin_date):
        """
        Pull data from interim database.

        :param instrument: str (Example: 'BTCUSD')
        :param evaluation_date: date
        :param begin_date: date
        :return: pd dataframe
        """
        data_set = (OrderFillEventDataModel()
                    .initialize(start_date=begin_date, evaluation_date=evaluation_date, other_condition="trading_pair = '{}'".format(instrument))
                    .evaluate()
                    .sort_values('order_fill_event_key'))

        unique_account_ids = data_set['account_id'].unique().tolist()

        account_info = (AccountDerivedMetaDataModel()
                        .initialize(other_condition='exchange_account_id in {}'.format(tuple(unique_account_ids)))
                        .evaluate())

        data_set = data_set.merge(account_info[['exchange_account_id', 'is_institutional']], left_on='account_id', right_on='exchange_account_id', how='left')

        data_set['event_date'] = data_set['created'].dt.date
        data_set['event_month'] = data_set['created'].dt.month
        data_set['event_day'] = data_set['created'].dt.day

        return data_set

    @timer
    def market_maker_trade_percentage(self, data_set):
        """
        Calculate market maker vs market taker of fill events ratio per account.

        :param data_set: pd dataframe
        :return: pd dataframe
        """
        def calculate_maker_vs_taker_percentage_per_group(data_set):
            maker_count = data_set.loc[data_set['liquidity_indicator'] == 'maker', 'count'].sum()
            taker_count = data_set.loc[data_set['liquidity_indicator'] == 'taker', 'count'].sum()
            total_count = maker_count + taker_count

            data_set['market_maker_trade_ratio'] = round(maker_count / total_count, 4)
            data_set['total_count'] = total_count

            return data_set

        matrix_data_set = (data_set
                           .groupby(['account_id', 'is_institutional', 'liquidity_indicator'], as_index=False)['order_id']
                           .count()
                           .rename(columns={'order_id': 'count'}))

        matrix_sub_data_set = matrix_data_set.loc[matrix_data_set['liquidity_indicator'].isin(['maker', 'taker'])]
        matrix_sub_data_set = matrix_sub_data_set.groupby(['account_id', 'is_institutional']).apply(calculate_maker_vs_taker_percentage_per_group)

        return matrix_sub_data_set

    @timer
    def plot_market_maker_ratio_graph(self, data_set, evaluation_date, begin_date, institution_only=True, plot_out=True):
        """
        Plot market maker or taker ratio of each account.

        :param data_set: pd dataframe
        :param evaluation_date: date
        :param begin_date: date
        :param institution_only: bool
        :param plot_out: bool
        """
        data_set_to_plot = data_set.loc[(data_set['liquidity_indicator'] == 'maker') | (data_set['market_maker_trade_ratio'] == 0)]
        data_set_to_plot['total_count_text'] = ('total count: ' + data_set_to_plot['total_count'].astype(str) +
                                                '<br>is institutional: ' + data_set_to_plot['is_institutional'].astype(str))
        data_set_to_plot = data_set_to_plot.sort_values('market_maker_trade_ratio')

        if institution_only:
            data_set_to_plot = data_set_to_plot.loc[data_set_to_plot['is_institutional']]

        # break down to account level
        data = [go.Scattergl(x=data_set_to_plot['account_id'],
                             y=data_set_to_plot['market_maker_trade_ratio'],
                             text=data_set_to_plot['total_count_text'],
                             mode='lines+markers')]

        layout = go.Layout(title='{} to {} Maker Percentage'.format(begin_date, evaluation_date),
                           xaxis={'type': 'category'},
                           yaxis={'tickformat': '.2%'})

        fig = go.Figure(data=data, layout=layout)
        fig.update_xaxes(title_text='Account ID')
        fig.update_yaxes(title_text='Maker Trade %')

        if plot_out:
            offplt.plot(fig, filename='maker_trade_{}_{}.html'.format(begin_date, evaluation_date))

        # histogram of account counts by maker trade %
        data_hist = [go.Histogram(x=data_set_to_plot['market_maker_trade_ratio'],
                                  xbins={'start': 0,
                                         'end': 1.1,
                                         'size': 0.1})]

        layout_hist = go.Layout(title='{} to {} Market Maker Trade Count'.format(begin_date, evaluation_date),
                                yaxis={'tickformat': ','})

        fig_hist = go.Figure(data=data_hist, layout=layout_hist)
        fig_hist.update_xaxes(title_text='Market Maker Trade %')
        fig_hist.update_yaxes(title_text='Account Count')

        if plot_out:
            offplt.plot(fig_hist, filename='market_maker_trade_count_{}_{}.html'.format(begin_date, evaluation_date))

        return fig, fig_hist

    @timer
    def plot_market_liquidity(self, data_set, evaluation_date, begin_date, plot_out=True):
        """
        Plot average trading volume for each of the order book.

        :param data_set: pd dataframe
        :param evaluation_date: date
        :param begin_date: date
        :param plot_out: bool
        """
        trade_volume_by_order_book = (data_set
                                      .groupby(['trading_pair', 'event_month'], as_index=False)
                                      .agg({'event_day': 'nunique', 'quantity': 'sum'})
                                      .rename(columns={'event_day': 'count_day_in_month', 'quantity': 'total_volume'}))

        # divide by 2 for adjusting double count
        trade_volume_by_order_book['avg_daily_trade_volume'] = trade_volume_by_order_book['total_volume'] / trade_volume_by_order_book['count_day_in_month'] / 2

        final_data_set = trade_volume_by_order_book.groupby('trading_pair', as_index=False)['avg_daily_trade_volume'].mean().round(2)

        data = [go.Bar(x=final_data_set['trading_pair'],
                       y=final_data_set['avg_daily_trade_volume'],
                       text=final_data_set['avg_daily_trade_volume'],
                       textposition='auto')]

        layout = go.Layout(title='Liquidity of Order Books ({} to {})'.format(begin_date, evaluation_date),
                           xaxis={'type': 'category'},
                           yaxis={'tickformat': ','})

        fig = go.Figure(data=data, layout=layout)
        fig.update_xaxes(title_text='Trading Pair')
        fig.update_yaxes(title_text='Average Trading Volume per Day')

        if plot_out:
            offplt.plot(fig, filename='liquidity_{}_{}.html'.format(begin_date, evaluation_date))

        return fig


# sample use case of this class
if __name__ == '__main__':
    calculation_module = MarketMakerAndLiquidity()

    instruments = CryptoPairsDataModel().evaluate()['trading_pair'].tolist()
    begin_date = datetime.date(2019, 1, 1)
    evaluation_date = datetime.date(2019, 8, 31)

    data_set_list = []
    for instrument in instruments:
        raw_data = calculation_module.get_data(instrument, evaluation_date, begin_date)
        data_set_list.append(raw_data)

    final_result = pd.concat(data_set_list)

    market_maker_data = calculation_module.market_maker_trade_percentage(final_result)
    calculation_module.plot_market_maker_ratio_graph(market_maker_data, evaluation_date, begin_date)

    calculation_module.plot_market_liquidity(final_result, evaluation_date, begin_date)
