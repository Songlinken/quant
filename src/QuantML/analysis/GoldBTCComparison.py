import datetime
import plotly.graph_objs as go
import plotly.offline as offplt

from src.QuantML.data_models.CCCAGGHistoryDataModel import CCCAGGHistoryDataModel
from src.QuantML.data_models.MetalHistoryDataModel import MetalHistoryDataModel


class GoldBTCComparison(object):

    def get_data(self, start_date, evaluation_date, instrument, metal):

        btc_history_data = (CCCAGGHistoryDataModel().initialize(start_date=start_date,
                                                                evaluation_date=evaluation_date,
                                                                other_condition="symbol = '{}'".format(instrument))
                                                    .evaluate())

        gold_history_data = (MetalHistoryDataModel().initialize(start_date=start_date,
                                                                evaluation_date=evaluation_date,
                                                                other_condition="metal = '{}'".format(metal))
                                                    .evaluate())

        result_dict = {instrument: btc_history_data, metal: gold_history_data}

        return result_dict

    def gold_btc_comparison(self, btc_data_set, gold_data_set, plot_out=True):

        btc_plt = go.Scattergl(x=btc_data_set['event_date'],
                                    y=btc_data_set['close_price'],
                                    mode='markers+lines',
                                    name='BTC')

        gold_plt = go.Scattergl(x=gold_data_set['event_date'],
                                y=gold_data_set['close_price'],
                                mode='markers+lines',
                                name='Gold')
        import ipdb;ipdb.set_trace()
        data = [btc_plt, gold_plt]
        layout = go.Layout(title='BTC vs Gold Price over Time')
        fig = go.Figure(data=data, layout=layout)

        if plot_out:
            offplt.plot(fig, filename='btc_vs_gold.html')

        # correlation test
        btc_gold_price = (btc_data_set[['event_date', 'close_price']]
                          .merge(gold_data_set[['event_date', 'close_price']], on='event_date', suffixes=('_btc', '_gold')))

        correlation = btc_gold_price[['close_price_btc', 'close_price_gold']].corr()

        return correlation


if __name__ == '__main__':
    model = GoldBTCComparison()
    data = model.get_data(start_date=datetime.date(2013, 1, 1),
                          evaluation_date=datetime.date(2019, 10, 30),
                          instrument='BTCUSD',
                          metal='gold')

    btc_gold_comparison = model.gold_btc_comparison(data['BTCUSD'], data['gold'])
