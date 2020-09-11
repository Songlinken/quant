import pandas as pd
import datetime

from src.alert_analysis.AnalysisNode import AnalysisNode
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.data_models.SmartsAlertsDataModel import SmartsAlertsDataModel
from src.data_models.SmartsDataModel import SmartsDataModel
from src.data_models.LastTradeAndTradeBackDetailDataModel import LastTradeAndTradeBackDetailDataModel
from src.utility.GeneralUtility import timer


class WashSaleAnalysis(AnalysisNode):
    """
    Analysis of wash sale alerts from SMARTS (code: 4040/4041/4042).
    """
    @timer
    def get_alert(self, evaluation_date, begin_date=None, instrument=None):
        """
        Pull corresponding alerts from SMARTS backend database.

        :param evaluation_date: date
        :param begin_date: date
        :param instrument: str (Example: "'BTCUSD', 'LTCUSD'")
        :return: pd dataframe
        """
        if instrument:
            symbol_condition = 'securitycode in ({})'.format(instrument)

        else:
            symbol_condition = '1=1'

        alerts_4040_4041_4042 = SmartsAlertsDataModel().initialize(
            evaluation_date=evaluation_date,
            start_date=begin_date,
            other_condition='code in (4040, 4041, 4042) and {}'.format(symbol_condition)
        ).evaluate()

        return alerts_4040_4041_4042

    @timer
    def get_model_validation_alerts(self, instrument, stored_type, release_version, evaluation_date, begin_date):
        """
        Trigger same alerts as SMARTS does for wash sale (code: 4040/4041/4042)

        :param instrument: str or list (Example: 'BTCUSD', ['BTCUSD', 'LTCUSD'])
        :param stored_type: str
        :param release_version: int
        :param evaluation_date: date
        :param begin_date: date
        :return: dict with pd dataframe
        """
        # raw data information from datalab
        data_set_base = SmartsDataModel().initialize(evaluation_date=evaluation_date,
                                                     begin_date=begin_date,
                                                     other_condition="symbol in {}".format(instrument).replace('[', '(').replace(']', ')')
                                                     ).evaluate()
        data_set_base = data_set_base.loc[data_set_base['event_type'] == 'Fill']

        # trade back records from database
        if instrument:
            if isinstance(instrument, str):
                instrument = [instrument]
            instrument = str(instrument).replace('[', '').replace(']', '')
            symbol_condition = 'symbol in ({})'.format(instrument)

        else:
            symbol_condition = '1=1'

        trade_back_details = LastTradeAndTradeBackDetailDataModel().initialize(
            stored_type=stored_type,
            release_version=release_version,
            evaluation_date=evaluation_date,
            start_date=begin_date,
            other_condition=symbol_condition
        ).evaluate()

        sub_trade_back_details = trade_back_details[['account_id', 'event_id', 'last_traded_price', 'price_change_pct',
                                                     'time_from_last_trade', 'trade_value', 'counter_party_account_id',
                                                     'trade_back_time', 'trade_time_range', 'trade_back_price', 'money_pass']]

        data_set = data_set_base.merge(sub_trade_back_details, how='left', on=['account_id', 'event_id'])

        # SMARTS does not take block trading into account for wash sale
        data_set = data_set.loc[~data_set['execution_options'].isin(['block', 'execution_options'])]

        # get alerts for each of the instrument
        alerts_dict = {}
        for instrument in data_set['symbol'].unique():
            alerts = data_set.loc[(~data_set['trade_time_range'].isna()) & (data_set['symbol'] == instrument)]
            alerts_dict.update({instrument: alerts})

        return alerts_dict

    @timer
    def get_top_accounts_of_alerts(self, data_set, instrument):

        if isinstance(instrument, str):
            instrument = [instrument]

        top_accounts_dict = {}
        top_account_pairs_dict = {}
        for pair in instrument:
            top_accounts = data_set.loc[data_set['symbol'] == pair, 'account_id'].value_counts().sort_values(ascending=False)
            top_accounts.index = top_accounts.index.set_names('account_id')
            top_accounts = top_accounts.reset_index(name='triggered_times')
            top_accounts_dict.update({pair: top_accounts})

            # top account pairs getting triggered for 4041/4042
            top_account_pairs = (data_set.loc[data_set['symbol'] == pair]
                                 .groupby(['account_id', 'counter_party_account_id'])['event_id']
                                 .count()
                                 .sort_values(ascending=False)
                                 .reset_index(name='triggered_times'))
            top_account_pairs_dict.update({pair: top_account_pairs})

        return top_accounts_dict, top_account_pairs_dict


# sample use case of this class
if __name__ == '__main__':
    instruments = CryptoPairsDataModel().evaluate()['trading_pair'].tolist()
    analysis_model = WashSaleAnalysis()

    model_alerts = analysis_model.get_model_validation_alerts(instruments, 'c', 1, datetime.date(2019, 2, 28), datetime.date(2019, 1, 1))
    alerts_data_set = pd.concat(model_alerts).reset_index(drop=True)

    top_accounts, top_account_pairs = analysis_model.get_top_accounts_of_alerts(alerts_data_set, instruments)
