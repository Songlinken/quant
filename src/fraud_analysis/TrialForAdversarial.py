import datetime
import numpy as np
import pandas as pd
import random
import warnings

from sklearn.ensemble import RandomForestClassifier
from imblearn.combine import SMOTEENN
from sklearn.metrics import confusion_matrix, precision_score, recall_score
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import LabelEncoder

from src.data_models.OrderFillEventDataModel import OrderFillEventDataModel
from src.data_models.RiskLevelDataModel import RiskLevelDataModel
from src.data_models.TransferEventDataModel import TransferEventDataModel
from src.utility.GeneralUtility import timer

warnings.filterwarnings("ignore")


class TrialForAdversarial(object):

    current_adversarial_accounts = [358343, 676163, 888892, 899154, 899363, 899420, 904247, 906100, 912450, 913171,
                                    915847, 917662, 920283, 921491, 924831, 931171, 933100, 937998, 940363, 942532, 943880,
                                    946431, 950159, 954475, 955081, 956140, 958124, 958824, 960539, 961987, 964576, 968476,
                                    977638, 981921, 993882, 1012529, 1035636, 1132314, 1139568, 1143127, 1264259, 1273541,
                                    1412708, 1435539, 1529168, 1885570, 2295654, 2451253]

    @timer
    def get_data(self, sample_account_number=10000):

        transfer_event_data = (TransferEventDataModel()
                               .initialize()
                               .evaluate())

        transfer_event_accounts = (transfer_event_data['account_id']
                                   .value_counts()
                                   .keys()
                                   .tolist())

        sample_accounts_collection = list(set(transfer_event_accounts) - set(self.current_adversarial_accounts))

        sample_accounts = random.sample(sample_accounts_collection, sample_account_number) + self.current_adversarial_accounts

        # get trade data for sample accounts
        order_fill_event_data = (OrderFillEventDataModel()
                                 .initialize(other_condition="account_id in {}".format(sample_accounts)
                                 .replace('[', '(')
                                 .replace(']', ')'))
                                 .evaluate())

        order_fill_event_data['notional_cost'] = order_fill_event_data['price'] * order_fill_event_data['quantity']
        order_fill_event_data.loc[order_fill_event_data['side'] == 'sell', 'notional_cost'] *= -1
        order_fill_event_data.loc[order_fill_event_data['side'] == 'sell', 'quantity'] *= -1
        order_fill_event_data['trading_pair'] = order_fill_event_data['trading_pair'].str[-3:]

        sub_order_fill_event_data = (order_fill_event_data[['event_id', 'account_id', 'created', 'trading_pair', 'notional_cost', 'order_type', 'quantity']]
                                     .rename({'created': 'tx_time',
                                              'trading_pair': 'currency',
                                              'notional_cost': 'amount',
                                              'order_type': 'type',
                                              'quantity': 'running_crypto'}, axis=1))

        sub_order_fill_event_data = sub_order_fill_event_data.assign(is_credit=None,
                                                                     is_advance=None,
                                                                     metadata=None,
                                                                     fee=np.nan)

        transfer_event_data['running_crypto'] = np.nan
        sub_transfer_data = transfer_event_data.loc[transfer_event_data['account_id'].isin(sample_accounts)]

        final_transfer_event_data = pd.concat([sub_transfer_data, sub_order_fill_event_data])

        # get risk data for sample accounts
        risk_info = (RiskLevelDataModel()
                     .initialize(other_condition='exchange_account_id in {}'.format(tuple(sample_accounts)))
                     .evaluate())

        risk_info = risk_info.rename({'exchange_account_id': 'account_id'}, axis=1)
        risk_info = risk_info.loc[risk_info['level'].isin(['HighRisk', 'MediumRisk', 'LowRisk'])]
        risk_info['level'] = pd.Categorical(risk_info['level'], ['HighRisk', 'MediumRisk', 'LowRisk'])
        risk_info = risk_info.sort_values(['account_id', 'level'])
        risk_info = risk_info.groupby('account_id', as_index=False)['level'].first()

        final_transfer_event_data = final_transfer_event_data.merge(risk_info[['account_id', 'level']], on='account_id', how='left')

        # drop unverified accounts
        final_transfer_event_data = final_transfer_event_data.loc[~final_transfer_event_data['level'].isna()]

        return final_transfer_event_data

    def random_forest_classifier(self,
                                 data_set,
                                 train_ratio=0.7,
                                 bootstrap=True,
                                 oob_score=False,
                                 criterion='gini',
                                 max_depth=4,
                                 n_estimators=10,
                                 grid_search=False):

        features = ['currency', 'level', 'deposit_trade_count_ratio', 'deposit_ratio_mean', 'sum_activity', 'trade_withdraw_in_two_weeks']

        sample_data_set = (data_set.loc[~data_set['first_trade_to_withdraw'].isna()]
                           .groupby('account_id', as_index=False)[features].first())

        sample_data_set.loc[sample_data_set['account_id'].isin(self.current_adversarial_accounts), 'increase_limit'] = True
        sample_data_set['increase_limit'] = sample_data_set['increase_limit'].fillna(False)

        # remove admin credit account with withdraw activity
        sample_data_set = sample_data_set.loc[~sample_data_set['deposit_ratio_mean'].isna()]

        # encoding
        le_currency = LabelEncoder()
        le_level = LabelEncoder()
        le_trade_withdraw_in_two_weeks = LabelEncoder()
        le_increase_limit = LabelEncoder()

        le_currency.fit(sample_data_set['currency'].unique())
        le_level.fit(sample_data_set['level'].unique())
        le_trade_withdraw_in_two_weeks.fit(sample_data_set['trade_withdraw_in_two_weeks'].unique())
        le_increase_limit.fit(sample_data_set['increase_limit'].unique())

        sample_data_set['currency'] = le_currency.transform(sample_data_set['currency'])
        sample_data_set['level'] = le_level.transform(sample_data_set['level'])
        sample_data_set['trade_withdraw_in_two_weeks'] = le_trade_withdraw_in_two_weeks.transform(sample_data_set['trade_withdraw_in_two_weeks'])
        sample_data_set['increase_limit'] = le_increase_limit.transform(sample_data_set['increase_limit'])

        # prepare train/test
        train_data = sample_data_set.sample(round(sample_data_set.shape[0] * train_ratio))
        test_data = sample_data_set.loc[~sample_data_set.index.isin(train_data.index)]

        # oversampling + undersampling
        sme = SMOTEENN()
        train_data, y_label = sme.fit_resample(train_data[features], train_data['increase_limit'])
        train_data['increase_limit'] = y_label

        # model
        clf = RandomForestClassifier(n_jobs=-1,
                                     bootstrap=bootstrap,
                                     oob_score=oob_score,
                                     criterion=criterion,
                                     max_depth=max_depth,
                                     n_estimators=n_estimators)

        if grid_search:
            param_grid = [{
                'criterion': ['gini', 'entropy'],
                'max_depth': [3, 4, 5, None],
                'n_estimators': [10, 20, 50, 100],
                'bootstrap': [True, False],
                'oob_score': [True, False],
                'n_jobs': [-1]
            }]

            grid = GridSearchCV(clf, param_grid, scoring='recall', cv=5)
            grid.fit(train_data[features], train_data['increase_limit'])
            print('Best CV score: {:.2f}'.format(grid.best_score_))
            print('Best parameters: ', grid.best_params_)
            test_data['model_result'] = grid.predict(test_data[features])

        else:
            clf.fit(train_data[features], train_data['increase_limit'])
            test_data['model_result'] = clf.predict(test_data[features])

        # result
        result_confusion_matrix = confusion_matrix(test_data['increase_limit'], test_data['model_result'])
        result_precision = precision_score(test_data['increase_limit'], test_data['model_result'])
        result_recall = recall_score(test_data['increase_limit'], test_data['model_result'])

        result = {'result_confusion_matrix': result_confusion_matrix,
                  'result_precision': result_precision,
                  'result_recall': result_recall}

        return result

    @timer
    def adversarial_feature_preparation(self, data_set, look_back_period=6):

        data_set = data_set.sort_values(['account_id', 'tx_time'])
        data_set = self.get_running_balance(data_set)
        data_set = self.get_trade_withdraw_time_diff(data_set)
        data_set = self.get_deposit_trade_withdraw_count(data_set)

        # first trade to withdraw
        account_first_trade_time = (data_set.loc[data_set['type'].isin(['limit', 'market'])]
                                    .groupby('account_id', as_index=False)['tx_time']
                                    .first()
                                    .rename({'tx_time': 'first_trade_time'}, axis=1))

        data_set = data_set.merge(account_first_trade_time, how='left', on='account_id')
        data_set.loc[data_set['type'] == 'Withdrawal', 'first_trade_to_withdraw'] = data_set['tx_time'] - data_set['first_trade_time']
        data_set.loc[data_set['first_trade_to_withdraw'] <= datetime.timedelta(0), 'first_trade_to_withdraw'] = pd.NaT
        data_set.loc[(~data_set['first_trade_to_withdraw'].isna()) & (data_set['first_trade_to_withdraw'] <= datetime.timedelta(days=14)), 'trade_withdraw_in_two_weeks'] = True
        data_set['trade_withdraw_in_two_weeks'] = data_set['trade_withdraw_in_two_weeks'].fillna(False)

        # first deposit to withdraw
        account_first_deposit_time = (data_set.loc[data_set['type'] == 'Deposit']
                                      .groupby('account_id', as_index=False)['tx_time']
                                      .first()
                                      .rename({'tx_time': 'first_deposit_time'}, axis=1))

        data_set = data_set.merge(account_first_deposit_time, how='left', on='account_id')
        data_set.loc[data_set['type'] == 'Withdrawal', 'first_deposit_to_withdraw'] = data_set['tx_time'] - data_set['first_deposit_time']
        data_set.loc[data_set['first_deposit_to_withdraw'] <= datetime.timedelta(0), 'first_deposit_to_withdraw'] = pd.NaT

        # running count
        data_set[['deposit_running_count', 'trade_running_count']] = data_set.groupby('account_id')[['deposit_running_count', 'trade_running_count']].ffill()
        data_set.loc[data_set['type'] == 'Deposit', 'deposit_ratio'] = data_set.loc[data_set['type'] == 'Deposit', 'amount'] / 500.0
        deposit_ratio_expanding_mean = (data_set.groupby('account_id')
                                        .expanding()
                                        .agg({'deposit_ratio': 'mean'})
                                        .rename({'deposit_ratio': 'deposit_ratio_mean'}, axis=1)
                                        .reset_index(drop=True))

        data_set = (data_set.reset_index()
                    .merge(deposit_ratio_expanding_mean, left_index=True, right_index=True)
                    .set_index('index'))

        data_set.index.name = None
        data_set['sum_activity'] = data_set['deposit_running_count'] + data_set['trade_running_count'] + data_set['withdraw_running_count']
        data_set.loc[data_set['type'].isin(['market', 'limit']), 'trade_count'] = 1
        data_set.loc[data_set['type'] == 'Deposit', 'deposit_count'] = 1
        data_set.loc[data_set['type'] == 'Withdrawal', 'withdraw_count'] = 1
        data_set['day_of_week'] = data_set['tx_time'].dt.dayofweek + 1
        data_set['begin_of_period'] = data_set.loc[data_set['day_of_week'] == 5, 'tx_time'] - datetime.timedelta(days=look_back_period)
        data_set['begin_date'] = pd.to_datetime(data_set['begin_of_period'].dt.date)
        data_set = data_set.sort_values('tx_time')
        data_set['begin_date'] = data_set['begin_date'].bfill()
        data_set['time_diff'] = data_set['tx_time'] - data_set['begin_date']

        # in case there is no Friday trade
        data_set = data_set.loc[data_set['time_diff'] > datetime.timedelta(0)]

        return data_set

    @timer
    def get_running_balance(self, data_set):
        """
        Calculate the running crypto/cash balance for each account.

        :param data_set: pd dataframe
        :return: pd dataframe
        """
        account_pair_list = [tuple(pair) for pair in data_set[['account_id', 'currency']].drop_duplicates().to_numpy()]

        data_set = data_set.reset_index()
        data_set = data_set.assign(running_balance=np.nan)

        columns = data_set.columns
        data_types = data_set.dtypes
        data_array = data_set.to_numpy()

        crypto_balance_dict = {account: 0 for account in account_pair_list}
        cash_balance_dict = {account: 0 for account in account_pair_list}

        # column 1 is account_id
        for row in range(data_array.shape[0]):
            # column 3 is currency
            if data_array[row, 3] == 'USD':
                # column 11 is type
                if data_array[row, 11] == 'Deposit':
                    # column 2 is amount
                    cash_balance_dict[(data_array[row, 1], data_array[row, 3])] += data_array[row, 2]
                    # column -1 is running_balance
                    data_array[row, -1] = cash_balance_dict[(data_array[row, 1], data_array[row, 3])]

                elif data_array[row, 11] in ['market', 'limit']:
                    # column 9 is running_crypto
                    cash_balance_dict[(data_array[row, 1], data_array[row, 3])] -= data_array[row, 2]
                    crypto_balance_dict[(data_array[row, 1], data_array[row, 3])] += data_array[row, 9]
                    data_array[row, -1] = cash_balance_dict[(data_array[row, 1], data_array[row, 3])]
            # should be crypto below
            else:
                crypto_balance_dict[(data_array[row, 1], data_array[row, 3])] -= data_array[row, 2]
                data_array[row, -1] = crypto_balance_dict[(data_array[row, 1], data_array[row, 3])]

        result_data_set = pd.DataFrame(np.row_stack(data_array), columns=columns)
        result_data_set = result_data_set.astype(data_types)
        result_data_set = result_data_set.set_index('index')
        result_data_set.index.name = None

        return result_data_set

    @timer
    def get_trade_withdraw_time_diff(self, data_set):
        """
        Calculate most recent trade to withdraw time range.

        :param data_set: pd dataframe
        :return: pd dataframe
        """
        account_pair_list = data_set['account_id'].value_counts().keys().tolist()

        data_set = data_set.reset_index()
        data_set = data_set.assign(recent_trade_to_withdraw=None)
        data_set['recent_trade_to_withdraw'] = data_set['recent_trade_to_withdraw'].astype('timedelta64')

        columns = data_set.columns
        data_types = data_set.dtypes
        data_array = data_set.to_numpy()

        recent_trade_time_dict = {account: pd.NaT for account in account_pair_list}

        for row in range(data_array.shape[0]):
            # column 1 is account_id
            if pd.isnull(recent_trade_time_dict[data_array[row, 1]]):
                # column 11 is type
                if data_array[row, 11] in ['market', 'limit']:
                    # column 10 is tx_time
                    recent_trade_time_dict[data_array[row, 1]] = data_array[row, 10]

            else:
                if data_array[row, 11] in ['market', 'limit']:
                    # column 10 is tx_time
                    recent_trade_time_dict[data_array[row, 1]] = data_array[row, 10]

                elif data_array[row, 11] == 'Withdrawal':
                    data_array[row, -1] = data_array[row, 10] - recent_trade_time_dict[data_array[row, 1]]
                    recent_trade_time_dict[data_array[row, 1]] = pd.NaT

        result_data_set = pd.DataFrame(np.row_stack(data_array), columns=columns)
        result_data_set = result_data_set.astype(data_types)
        result_data_set = result_data_set.set_index('index')
        result_data_set.index.name = None

        return result_data_set

    @timer
    def get_deposit_trade_withdraw_count(self, data_set):
        """
        Calculate deposit count over trade per account.

        :param data_set: pd dataframe
        :return: pd dataframe
        """
        account_pair_list = data_set['account_id'].value_counts().keys().tolist()

        data_set = data_set.reset_index()
        data_set = data_set.assign(deposit_running_count=np.nan,
                                   trade_running_count=np.nan,
                                   withdraw_running_count=np.nan,
                                   deposit_trade_count_ratio=np.nan)

        columns = data_set.columns
        data_types = data_set.dtypes
        data_array = data_set.to_numpy()

        account_deposit = {account: 0 for account in account_pair_list}
        account_trade = {account: 0 for account in account_pair_list}
        account_withdraw = {account: 0 for account in account_pair_list}

        # column 1 is account_id
        for row in range(data_array.shape[0]):
            # column 11 is type
            if data_array[row, 11] == 'Deposit':
                account_deposit[data_array[row, 1]] += 1
                data_array[row, -4] = account_deposit[data_array[row, 1]]
                data_array[row, -3] = account_trade[data_array[row, 1]]
                data_array[row, -2] = account_withdraw[data_array[row, 1]]

            elif data_array[row, 11] in ['limit', 'market']:
                account_trade[data_array[row, 1]] += 1
                data_array[row, -4] = account_deposit[data_array[row, 1]]
                data_array[row, -3] = account_trade[data_array[row, 1]]
                data_array[row, -2] = account_withdraw[data_array[row, 1]]

            elif data_array[row, 11] == 'Withdrawal':
                account_withdraw[data_array[row, 1]] += 1
                data_array[row, -4] = account_deposit[data_array[row, 1]]
                data_array[row, -3] = account_trade[data_array[row, 1]]
                data_array[row, -2] = account_withdraw[data_array[row, 1]]

            if account_trade[data_array[row, 1]] != 0:
                data_array[row, -1] = account_deposit[data_array[row, 1]] / account_trade[data_array[row, 1]]

        result_data_set = pd.DataFrame(np.row_stack(data_array), columns=columns)
        result_data_set = result_data_set.astype(data_types)
        result_data_set = result_data_set.set_index('index')
        result_data_set.index.name = None

        return result_data_set


if __name__ == '__main__':
    model = TrialForAdversarial()
    data = model.get_data()

    # Adversarial features
    adversarial_data = model.adversarial_feature_preparation(data)
    final_result = model.random_forest_classifier(adversarial_data, grid_search=True)
