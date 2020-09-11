import datetime
import dash_core_components as dcc
import dash_html_components as html


def make_html_calendar(alert_code):
    calendar_ = html.Div([
        dcc.DatePickerSingle(
            id='date_picker_{}'.format(alert_code),
            min_date_allowed=datetime.date(2019, 1, 1),
            max_date_allowed=datetime.date.today() - datetime.timedelta(days=1),
            date=datetime.date.today() - datetime.timedelta(days=1),
            display_format='MMM Do, YYYY',
            with_portal=True)
    ])
    return calendar_
