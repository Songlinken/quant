import datetime
import numpy as np
import os
import pandas as pd
import slack

from src.alert_analysis.WashSaleAnalysis import WashSaleAnalysis
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.data_models.SmartsAlertsDataModel import SmartsAlertsDataModel
from src.utility.GeneralUtility import get_first_day_of_last_month, get_last_day_of_last_month


class MktsrvAlertReport(object):
    """
    Send official alerts information to specific Slack channel.
    """
    alert_id_mapping = {
        1001: '1001-Unusual Price Movement Intra-Day',
        2011: '2011-Unusual Volume Intra-Day',
        4009: '4009-Painting the Tape',
        4011: '4011-Crossing at Short Term High/Low',
        4012: '4012-Price Driver',
        4022: '4022-Bait and Switch',
        4023: '4023-Layering Repeat',
        4032: '4032-Multi Order Spoofing with Bait and Switch',
        4040: '4040-Wash Sales A - A',
        4041: '4041-Wash Sales A - B - A',
        4042: '4042-Wash Sales A - B - A (Money Pass)',
        4045: '4045-Collusion',
        4047: '4047-Pre-arranged Trade'
    }

    def report_to_slack_channel(self, msg, channel='mktsrv', icon=':robot_face:'):
        """
        Initialize slack client object.
        """
        slack_client = slack.WebClient(os.environ['SLACK_BOT_TOKEN'])

        slack_client.chat_postMessage(
            channel=channel,
            text=msg,
            icon_emoji=icon
        )

    def get_bot_msg_from_slack_channel(self, channel='GH50J4WLQ'):
        """
        Get msg from slack channel, need use USER_TOKEN to get historical chat record and locate ts which need to be
        passed into slack chat.delete API.
        """
        slack_client = slack.WebClient(os.environ['SLACK_USER_TOKEN'])

        chat_history = slack_client.groups_history(
            channel=channel
        )['messages']

        bot_msg = [msg for msg in chat_history if 'subtype' in msg and msg['subtype'] == 'bot_message']

        return bot_msg

    def delete_bot_msg_from_slack_channel(self, ts, channel='GH50J4WLQ'):
        """
        Use get_bot_msg_from_slack_channel to get ts.
        """
        slack_client = slack.WebClient(os.environ['SLACK_BOT_TOKEN'])

        slack_client.chat_delete(
            channel=channel,
            ts=ts
        )

    def get_wash_model_alert_count(self, evaluation_date=None, stored_type='c', release_version=1):
        """
        Get wash aba alerts from validation model.
        :param evaluation_date: date
        :param stored_type: str
        :param release_version: int
        :return: dict
        """
        if not evaluation_date:
            evaluation_date = datetime.date.today() - datetime.timedelta(days=1)

        instruments = CryptoPairsDataModel().evaluate()['trading_pair'].tolist()

        model_alerts = WashSaleAnalysis().get_model_validation_alerts(instruments, stored_type, release_version, evaluation_date, evaluation_date)
        alerts_data_set = pd.concat(model_alerts).reset_index(drop=True)

        alert_4040_count = alerts_data_set.loc[alerts_data_set['account_id'] == alerts_data_set['counter_party_account_id']].shape[0] / 2
        alert_4041_count = alerts_data_set.loc[alerts_data_set['money_pass'] == 0].shape[0] / 2
        alert_4042_count = alerts_data_set.loc[(alerts_data_set['account_id'] != alerts_data_set['counter_party_account_id']) & (alerts_data_set['money_pass'] != 0)].shape[0] / 2

        # SMARTS grouped alert 4041/4042
        if alert_4042_count >= 1:
            alert_4042_count = 1

        if alert_4041_count >= 1:
            alert_4041_count = 1

        wash_sale_count = {'alert_4040': alert_4040_count,
                           'alert_4041': alert_4041_count,
                           'alert_4042': alert_4042_count}

        return wash_sale_count

    def get_model_alert_summary(self, evaluation_date=None):
        """
        Get alerts count dataframe from analytical model.
        """
        alert_types = list(self.alert_id_mapping.values())
        alert_types.append('total_alerts')
        data_frame_to_report = pd.DataFrame({'Name': alert_types, 'Model_Validation': [np.nan] * 14})

        # get_wash model alert count
        wash_alerts = self.get_wash_model_alert_count(evaluation_date)
        data_frame_to_report.loc[data_frame_to_report['Name'] == '4040-Wash Sales A - A', 'Model_Validation'] = wash_alerts['alert_4040']
        data_frame_to_report.loc[data_frame_to_report['Name'] == '4041-Wash Sales A - B - A', 'Model_Validation'] = wash_alerts['alert_4041']
        data_frame_to_report.loc[data_frame_to_report['Name'] == '4042-Wash Sales A - B - A (Money Pass)', 'Model_Validation'] = wash_alerts['alert_4042']

        # add total to dataframe
        data_frame_to_report.loc[data_frame_to_report['Name'] == 'total_alerts', 'Model_Validation'] = data_frame_to_report['Model_Validation'].sum()

        return data_frame_to_report

    def get_official_alert_summary(self, report_type='daily'):
        """
        Get alerts count dataframe from SMARTS backend database.
        """
        if report_type == 'daily':
            evaluation_date = datetime.date.today() - datetime.timedelta(days=1)
            other_condition = 'isreissue is False'

            alerts_data_set = (SmartsAlertsDataModel()
                               .initialize(evaluation_date=evaluation_date, other_condition=other_condition)
                               .evaluate())

        elif report_type == 'monthly':
            evaluation_date = get_last_day_of_last_month()
            start_date = get_first_day_of_last_month()
            other_condition = 'isreissue is False'

            alerts_data_set = (SmartsAlertsDataModel()
                               .initialize(start_date=start_date, evaluation_date=evaluation_date, other_condition=other_condition)
                               .evaluate())

        else:
            raise ValueError('report_type should be either "daily" or "monthly"')

        alerts_count_by_group = alerts_data_set.groupby('code')['id'].count().reset_index(name='Official')
        alerts_count_by_group['code'] = alerts_count_by_group['code'].map(self.alert_id_mapping)
        alerts_count_by_group = alerts_count_by_group.rename(columns={'code': 'Name'})

        # dataframe to report
        data_frame_to_report = pd.DataFrame({'Name': list(self.alert_id_mapping.values())})
        data_frame_to_report = data_frame_to_report.merge(alerts_count_by_group, how='left', on='Name').fillna(0)
        data_frame_to_report['Official'] = data_frame_to_report['Official'].astype('int64')

        # add total to dataframe
        data_frame_to_report.loc['sum'] = data_frame_to_report['Official'].sum()
        data_frame_to_report.loc[data_frame_to_report.index[-1], 'Name'] = 'total_alerts'

        return data_frame_to_report

    def prepare_txt_from_df_to_report(self, data_set, evaluation_date, report_type='daily'):
        """
        Convert pandas dataframe to txt format for reporting on slack channel.
        :param data_set: pd dataframe
        :param evaluation_date: date
        :param report_type: str
        :return: str
        """
        evaluation_date = evaluation_date - datetime.timedelta(days=1)

        # text format
        if report_type == 'daily':
            text_time = '`' + evaluation_date.strftime("%Y-%m-%d") + '`\n'

        elif report_type == 'monthly':
            text_time = '`' + evaluation_date.strftime("%Y-%b") + '`\n'

        else:
            raise ValueError('report_type can only be "daily" or "monthly".')

        text_file = '``` ' + data_set.to_string(formatters={'Name': '{{:<{}s}}'.format(data_set['Name'].str.len().max()).format}, index=False, justify='left') + '```'

        text_formatted = text_time + text_file

        return text_formatted


if __name__ == '__main__':
    model = MktsrvAlertReport()
    current_date = datetime.date.today()

    # report on previous date since database does not have data for current date
    official_alert_df = model.get_official_alert_summary(report_type='daily')
    model_alert_df = model.get_model_alert_summary()
    report_df = official_alert_df.merge(model_alert_df, how='left', on='Name')
    report_msg = model.prepare_txt_from_df_to_report(report_df, current_date)
    model.report_to_slack_channel(report_msg)

    # report last month's alerts if current date is the first day of the month
    if current_date == datetime.date.today().replace(day=1):
        report_df = model.get_official_alert_summary(report_type='monthly')
        report_msg = model.prepare_txt_from_df_to_report(report_df, current_date, 'monthly')
        model.report_to_slack_channel(report_msg)
