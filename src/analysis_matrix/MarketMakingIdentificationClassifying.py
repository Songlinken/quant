import datetime
import pandas as pd
import warnings

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, precision_score, recall_score
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import MinMaxScaler

from src.data_models.MarketMakerIdentificationMatrixDataModel import MarketMakerIdentificationMatrixDataModel
from src.utility.GeneralUtility import timer

warnings.filterwarnings("ignore")


class MarketMakingIdentificationClassifying(object):

    @timer
    def get_data(self, instrument, start_date, evaluation_date):

        data_set = (MarketMakerIdentificationMatrixDataModel()
                    .initialize(start_date=start_date,
                                evaluation_date=evaluation_date,
                                other_condition="symbol = '{}'".format(instrument))
                    .evaluate())

        return data_set

    @timer
    def feature_preparation(self, data_set):

        data_set['cancel_ratio'] = data_set['cancel_orders'] / data_set['total_orders']

        data_set.fillna({'place_cancel_time_diff': 10000,
                         'bid_ask_ratio_median': 10000,
                         'bid_ask_ratio_std': 0,
                         'fill_buy_orders_count': 0,
                         'fill_sell_orders_count': 0,
                         'fill_buy_orders_quantity': 0,
                         'fill_sell_orders_quantity': 0,
                         'cancel_orders': 0,
                         'cancel_ratio': 0,
                         'maker_counts': 0,
                         'taker_counts': 0}, inplace=True)

        data_set['fill_buy_sell_diff'] = data_set['fill_buy_orders_quantity'] - data_set['fill_sell_orders_quantity']

        return data_set

    @timer
    def random_forest_analysis(self, data_set, train_ratio=0.7, iteration=5):

        for round_ in range(iteration):
            train_data = data_set.sample(round(data_set.shape[0] * train_ratio))
            test_data = data_set.loc[~data_set.index.isin(train_data.index)]

            features = ['place_cancel_time_diff',
                        'bid_ask_ratio_median',
                        'bid_ask_ratio_std',
                        'cancel_ratio',
                        'cancel_orders',
                        'fill_buy_sell_diff']

            labels, _ = pd.factorize(train_data['market_maker'])

            # train model
            clf = RandomForestClassifier(n_jobs=-1)
            clf.fit(train_data[features], labels)

            # prediction
            test_data['random_forest_result'] = clf.predict(test_data[features])

            # result
            result_confusion_matrix = confusion_matrix(test_data['market_maker'], test_data['random_forest_result'])
            result_precision = precision_score(test_data['market_maker'], test_data['random_forest_result'])
            result_recall = recall_score(test_data['market_maker'], test_data['random_forest_result'])

            print('Iteration_{}:'.format(round_ + 1), '\n',
                  'Confusion_matrix: ', result_confusion_matrix, '\n',
                  'Precision: ', result_precision, '\n',
                  'Recall: ', result_recall)

    @timer
    def knn_analysis(self, data_set, test_ratio=0.2):

        features = ['place_cancel_time_diff',
                    'bid_ask_ratio_median',
                    'bid_ask_ratio_std',
                    'cancel_ratio',
                    'cancel_orders',
                    'fill_buy_sell_diff']

        data_set[features] = MinMaxScaler().fit_transform(data_set[features])

        labels, _ = pd.factorize(data_set['market_maker'])

        train_x, test_x, train_y, test_y = train_test_split(data_set[features], labels, test_size=test_ratio)

        # train model
        clf = KNeighborsClassifier(n_neighbors=3, n_jobs=-1)
        clf.fit(train_x, train_y)

        # prediction


# sample use case
if __name__ == '__main__':
    module = MarketMakingIdentificationClassifying()
    data_set = module.get_data('BTCUSD', datetime.date(2019, 1, 1), datetime.date(2019, 9, 30))
    feature_test = module.feature_preparation(data_set)
    # module.random_forest_analysis(feature_test)
    module.knn_analysis(feature_test)
