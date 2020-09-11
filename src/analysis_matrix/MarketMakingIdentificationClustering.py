import datetime
import ipdb
import numpy as np
import plotly.graph_objs as go
import plotly.offline as offplt

from scipy.spatial.distance import cdist
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine

from src.database.SSHtunnel import SSHManager
from src.data_models.MarketMakerIdentificationMatrixDataModel import MarketMakerIdentificationMatrixDataModel
from src.utility.Configuration import Configuration
from src.utility.DataModelUtility import data_frame_to_sql
from src.utility.GeneralUtility import timer
from src.utility.MathUtility import sigmoid


offplt.offline.init_notebook_mode(connected=True)


class MarketMakingIdentificationClustering(object):

    @timer
    def get_data(self, instrument, start_date, evaluation_date):

        data_set = (MarketMakerIdentificationMatrixDataModel()
                    .initialize(start_date=start_date,
                                evaluation_date=evaluation_date,
                                other_condition="symbol = '{}'".format(instrument))
                    .evaluate())

        return data_set

    @timer
    def feature_preparation(self, data_set, plot_out=False):

        data_set['cancel_ratio'] = data_set['cancel_orders'] / data_set['total_orders']

        # spoofing need cancel event & fill event
        sub_data_set = data_set.loc[~data_set['place_cancel_time_diff'].isna()]
        sub_data_set = sub_data_set.loc[(~sub_data_set['fill_buy_orders_quantity'].isna()) |
                                        (~sub_data_set['fill_sell_orders_quantity'].isna())]

        sub_data_set.fillna({'bid_ask_ratio_std': 0,
                             'fill_buy_orders_quantity': 0,
                             'fill_sell_orders_quantity': 0}, inplace=True)

        sub_data_set['fill_buy_sell_diff'] = sub_data_set['fill_buy_orders_quantity'] - sub_data_set['fill_sell_orders_quantity']

        # normalization
        feature_data_set = sub_data_set[['account_id',
                                         'data_from_date',
                                         'place_cancel_time_diff',
                                         'bid_ask_ratio_median',
                                         'cancel_ratio',
                                         'cancel_orders',
                                         'fill_buy_sell_diff']]

        feature_data_set['bid_ask_ratio_median'] = sigmoid(feature_data_set['bid_ask_ratio_median'], 2, 1, -1)
        feature_data_set['fill_buy_sell_diff'] = sigmoid(abs(feature_data_set['fill_buy_sell_diff']), 2, 1, -1)
        feature_data_set[['place_cancel_time_diff', 'cancel_orders']] = (MinMaxScaler().fit_transform(feature_data_set[['place_cancel_time_diff', 'cancel_orders']]))

        # clustering
        optimal_clusters = self.elbow_method(feature_data_set[['place_cancel_time_diff',
                                                               'bid_ask_ratio_median',
                                                               'fill_buy_sell_diff',
                                                               'cancel_orders',
                                                               'cancel_ratio']],
                                             plot_out=plot_out)

        kmeans_result = KMeans(n_clusters=4).fit_predict(feature_data_set[['place_cancel_time_diff',
                                                                                          'bid_ask_ratio_median',
                                                                                          'fill_buy_sell_diff',
                                                                                          'cancel_orders',
                                                                                          'cancel_ratio']])

        sub_data_set['kmeans_result'] = kmeans_result

        final_result = data_set.merge(sub_data_set[['account_id',
                                                    'data_from_date',
                                                    'symbol',
                                                    'fill_buy_sell_diff',
                                                    'kmeans_result']],
                                      on=['account_id', 'data_from_date', 'symbol'],
                                      how='left')

        final_result['data_from_date'] = final_result['data_from_date'].dt.date
        ipdb.set_trace()
        return final_result

    def elbow_method(self, feature_data_set, cluster_centers=10, plot_out=True):

        distortions = []

        for cluster in range(1, cluster_centers):
            kmeans_model = KMeans(n_clusters=cluster).fit(feature_data_set)
            kmeans_model.fit(feature_data_set)
            distortions.append(sum(np.min(cdist(feature_data_set, kmeans_model.cluster_centers_, 'euclidean'), axis=1)) / feature_data_set.shape[0])

        if plot_out:
            layout = go.Layout(title='The Elbow Method Showing Optimal K')

            plot_object = go.Scattergl(x=list(range(1, cluster_centers)),
                                       y=distortions,
                                       name='cluster',
                                       mode='markers+lines')

            fig = go.Figure(data=plot_object, layout=layout)

            offplt.plot(fig, filename='elbow_method_result.html')

        decreasing_error = [distortions[i] - distortions[i + 1] for i in range(len(distortions) - 1)]

        return decreasing_error.index(min(decreasing_error))

    def update_market_maker_on_db(self, clustering_result, ssh='interim', database='gemrdsdb', host='localhost', port=55432):

        clustering_result = clustering_result[['account_id',
                                               'data_from_date',
                                               'symbol',
                                               'place_cancel_time_diff',
                                               'bid_ask_ratio_median',
                                               'bid_ask_ratio_std',
                                               'is_institutional',
                                               'fill_buy_orders_count',
                                               'fill_sell_orders_count',
                                               'fill_buy_orders_quantity',
                                               'fill_sell_orders_quantity',
                                               'total_orders',
                                               'cancel_orders',
                                               'maker_counts',
                                               'taker_counts',
                                               'market_maker']]

        data_frame_to_sql(clustering_result, 'temp_market_maker_result')

        query = """
                update   ms_dev.market_maker_identification_matrix as mmim
                set      market_maker = tmmr.market_maker
                from     ms_dev.temp_market_maker_result as tmmr
                where    mmim.account_id = tmmr.account_id
                and      mmim.data_from_date = tmmr.data_from_date
                and      mmim.symbol = tmmr.symbol;
                
                delete from ms_dev.temp_market_maker_result
                """

        configuration = Configuration.get()
        dbs = configuration.get('databases')
        db_exist = next((item for item in dbs if item["name"] == database), None)
        if not db_exist:
            raise Exception('Postgres database configuration of {} is not set properly.'.format(database))

        configuration = Configuration.get()
        ssh_tunnels = configuration.get('ssh_tunnel')
        ssh_exist = next((item for item in ssh_tunnels if item["name"] == ssh), None)
        ssh_tunnel = SSHManager(ssh_exist)
        ssh_tunnel.ssh_connect()

        engine = create_engine('postgresql+psycopg2://' + db_exist['username'] + ':' + db_exist['password'] + '@{}:{}/'.format(host, port) + database)
        connection = engine.raw_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()
        ssh_tunnel.connection.close()

        return clustering_result


# sample use case
if __name__ == '__main__':
    module = MarketMakingIdentificationClustering()
    data_set = module.get_data('BTCUSD', datetime.date(2019, 9, 1), datetime.date(2019, 9, 30))
    feature_test = module.feature_preparation(data_set)
    module.update_market_maker_on_db(feature_test)
