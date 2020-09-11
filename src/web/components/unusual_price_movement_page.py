import dash_html_components as html
import dash_table as dst
import pandas as pd

from src.data_models.SmartsAlertsDataModel import SmartsAlertsDataModel
from src.redis.RedisDataBase import RedisDatabase
from src.web.components.make_html_calendar import make_html_calendar


def unusual_price_movement_page():

    unusual_price_movement_page_lay_out = html.Div([

        # header
        html.Div([
            get_header()
        ]),

        # div 1: description
        html.Div([
            html.H4('Unusual Price Movement Intra-Day'),
            html.P("""The “Unusual Price Movement Intra-Day” alert identifies unusual price movements against
                      security-specific benchmarks for multiple configurable intra-day time ranges.
                   """),
            html.P("""E.g. for 10 minute and one hour intervals. When issued, the alert reports the participant who
                      caused the most number of price upticks or downticks over the time range that triggered the alert,
                      and therefore is most likely to be responsible for the unusual price movement.
                   """)
        ]),
        html.Br(),

        # calendar
        html.Div([
           make_html_calendar(1001)
        ]),
        html.Div(id='output_date_1001', style={'display': 'none'}),
        html.Br(),

        # div 2: alerts from validation_model
        html.Div([
            html.H4('Alerts Detail'),
            dst.DataTable(
                id='alerts_1001',
                data=pd.DataFrame().to_dict('rows'),
                columns=[{'id': c, 'name': c} for c in pd.DataFrame().columns],
                n_fixed_rows=1,
                style_data={'whiteSpace': 'normal'},
                style_cell={'textAlign': 'left'},
                style_cell_conditional=[{
                    'if': {'row_index': 'odd'},
                    'backgroundColor': 'rgb(248, 248, 248)'
                }],
                style_table={
                    'maxHeight': '300',
                    'overflowY': 'scroll'
                },
                css=[{
                    'selector': '.dash-cell div.dash-cell-value',
                    'rule': 'display: inline; white-space: pre; overflow: inherit; text-overflow: inherit;'
                }]
            )
        ])
    ])

    return unusual_price_movement_page_lay_out


def get_header():
    header = html.Div([
        html.Div([
            html.H2(children='1001: Unusual Price Movement Intra-Day',
                    style={
                        'textAlign': 'left'
                    })
        ])
    ])
    return header


def get_alerts_1001(evaluation_date, cache_result=True):

    redis_conn = RedisDatabase().initialize()
    cached_data = redis_conn.get('unusual_price_movement_alerts_' + evaluation_date.strftime('%Y_%m_%d'))

    if cached_data:
        cached_data = pd.read_msgpack(cached_data)
        return cached_data

    # all available instruments
    other_condition = 'code = 1001 and isreissue is False'

    alerts_data_set = (SmartsAlertsDataModel()
                       .initialize(evaluation_date=evaluation_date, other_condition=other_condition)
                       .evaluate())

    result = alerts_data_set['longtext'].to_frame('Alerts on {}'.format(evaluation_date))

    if result.empty:
        result = pd.DataFrame({'Alerts on {}'.format(evaluation_date): []})

    if cache_result:
        redis_conn.set('unusual_price_movement_alerts_' + evaluation_date.strftime('%Y_%m_%d'), result.to_msgpack(compress='zlib'))

    return result
