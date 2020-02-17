import datetime
import gspread
import os
import pandas as pd

from oauth2client.service_account import ServiceAccountCredentials

from src.data_models.SmartsAlertsDataModel import SmartsAlertsDataModel


class AlertsUpdates(object):
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

    def __init__(self):
        self.evaluation_date = datetime.date.today() - datetime.timedelta(days=1)

    def get_daily_alerts(self, evaluation_date=None):
        """
        Get daily alerts from SMARTS backend database.
        """
        if not evaluation_date:
            evaluation_date = self.evaluation_date

        other_condition = 'isreissue is False'

        daily_alerts = (SmartsAlertsDataModel()
                        .initialize(evaluation_date=evaluation_date, other_condition=other_condition)
                        .evaluate())

        alerts_count_by_group = daily_alerts.groupby('code')['id'].count().reset_index(name='Official')
        alerts_count_by_group['code'] = alerts_count_by_group['code'].map(self.alert_id_mapping)
        alerts_count_by_group = alerts_count_by_group.rename(columns={'code': 'Name'})

        # dataframe to report
        alert_data_frame = pd.DataFrame({'Name': list(self.alert_id_mapping.values())})
        alert_data_frame = alert_data_frame.merge(alerts_count_by_group, how='left', on='Name').fillna(0)
        alert_data_frame['Official'] = alert_data_frame['Official'].astype('int64')

        # add total to dataframe
        alert_data_frame.loc['sum'] = alert_data_frame['Official'].sum()
        alert_data_frame.loc[alert_data_frame.index[-1], 'Name'] = 'total_alerts'

        return alert_data_frame

    def update_alerts_on_gsheet(self, date_set, evaluation_date=None, google_sheet='SMARTS_alerts_records', work_sheet='Sheet1'):
        if not evaluation_date:
            evaluation_date = self.evaluation_date
        evaluation_date = str(evaluation_date)

        # initialize object with operating google sheet on google drive
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(os.getcwd() + '/google_credential.json', scope)
        gs = gspread.authorize(credentials)

        gsheet = gs.open(google_sheet)
        wsheet = gsheet.worksheet(work_sheet)
        available_row = self.next_available_row(wsheet)
        daily_alert_counts = date_set['Official'].tolist()

        # order mush be matched up with the alert_type listed in google sheet
        wsheet.update_cell(available_row, 1, evaluation_date)
        for index_, alert_count in enumerate(daily_alert_counts):
            wsheet.update_cell(available_row, index_ + 2, alert_count)

    def next_available_row(self, worksheet):
        str_list = list(filter(None, worksheet.col_values(1)))
        return len(str_list) + 1


if __name__ == '__main__':
    model = AlertsUpdates()
    alerts = model.get_daily_alerts()
    model.update_alerts_on_gsheet(alerts)
