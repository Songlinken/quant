import pandas as pd
import datetime
import re

from src.alert_analysis.AnalysisNode import AnalysisNode
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.data_models.SmartsAlertsDataModel import SmartsAlertsDataModel
from src.data_models.SmartsDataModel import SmartsDataModel
from src.utility.GeneralUtility import date_range, timer


class BaitAndSwitchAnalysis(AnalysisNode):
    """
    Analysis of bait and switch alerts from SMARTS (code: 4022).
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

        alerts_4022 = SmartsAlertsDataModel().initialize(
            evaluation_date=evaluation_date,
            start_date=begin_date,
            other_condition='code = 4022 and {}'.format(symbol_condition)
        ).evaluate()

        return alerts_4022

    @timer
    def false_positive_scenario_one(self, evaluation_date):
        """
        This function detects false positive 4022 alerts which has both fill sell/buy during the triggered bait and switch order.

        :param account_id: int
        :param evaluation_date: date
        :param instrument: str
        :return:
        """
        order_book_data = SmartsDataModel().initialize(
            evaluation_date=evaluation_date
        ).evaluate()

        order_book_data['date_time'] = pd.to_datetime(order_book_data['event_date'].astype('str') + ' ' + order_book_data['event_time'].astype(str)) + order_book_data['event_millis']

        alerts = self.get_alert(evaluation_date)

        close_cases = []
        for row in alerts.itertuples():
            alert_detail = getattr(row, 'longtext')

            account_id = int(getattr(row, 'housecode'))
            symbol = getattr(row, 'securitycode')
            alert_start_time = datetime.datetime.strptime(re.findall(r'\d{2}:\d{2}:\d{2}.\d{3}', alert_detail)[0], '%H:%M:%S.%f').time()
            alert_end_time = datetime.datetime.strptime(re.findall(r'\d{2}:\d{2}:\d{2}.\d{3}', alert_detail)[1], '%H:%M:%S.%f').time()

            fill_events_between_start_and_end = order_book_data.loc[(order_book_data['date_time'].dt.time <= alert_end_time) &
                                                                    (order_book_data['date_time'].dt.time >= alert_start_time) &
                                                                    (order_book_data['event_type'] == 'Fill') &
                                                                    (order_book_data['account_id'] == account_id) &
                                                                    (order_book_data['symbol'] == symbol)]

            if fill_events_between_start_and_end['side'].value_counts().size > 1:
                close_cases.append('Case closed because both sell & buy events got filled before the order cancelled.')

            else:
                close_cases.append(None)

        alerts['case_status'] = close_cases

        return alerts


# sample use case of this class
if __name__ == '__main__':
    instruments = CryptoPairsDataModel().evaluate()['trading_pair'].tolist()
    analysis_model = BaitAndSwitchAnalysis()

    result = []
    for date_ in date_range(datetime.date(2019, 5, 1), datetime.date(2019, 5, 31)):
        alerts = analysis_model.false_positive_scenario_one(evaluation_date=date_)
        result.append(alerts)

    analysis_result = pd.concat(result)
