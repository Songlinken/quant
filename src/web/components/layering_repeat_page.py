import dash_core_components as dcc
import dash_html_components as html
import dash_table as dst
import json
import pandas as pd
import plotly.graph_objs as go

from src.data_models.SmartsAlertsDataModel import SmartsAlertsDataModel
from src.redis.RedisDataBase import RedisDatabase
from src.reports.OrderBookSnapShoot import order_book_snap_shoot
from src.utility.GeneralUtility import ComplexEncoder
from src.web.components.make_html_calendar import make_html_calendar


def layering_repeat_page():

    layering_repeat_page_lay_out = html.Div([

        # header
        html.Div([
            get_header()
        ]),

        # div 1: description
        html.Div([
            html.H4('Layering Repeat'),
            html.P("""The Layering Alert identifies participants that enter orders at multiple price steps on one side
                      of the order book, and then wait for other participants to enter orders ahead of them in the queue,
                      before buying or selling back to these same participants.
                   """),
            html.P("""This alert also detects layered positions on both sides of the order book by created by a
                      participant alternating between entering buy and sell orders.
                   """)
        ]),
        html.Br(),

        # calendar
        html.Div([
           make_html_calendar(4023)
        ]),
        html.Div(id='output_date_4023', style={'display': 'none'}),
        html.Br(),

        # div 2: alerts from validation_model
        html.Div([
            html.H4('Alerts Detail'),
            dst.DataTable(
                id='alerts_4023',
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
        ]),
        html.Br(),

        # div 3: details of account action within the day
        html.Div([
            html.H4('Snapshoot of Best Prices'),
            html.H5('Instrument', style={'display': 'inline-block', 'width': 100, 'margin': 0, 'padding': 0, 'border': 0}),
            dcc.Input('layering_repeat_instrument', type='text', style={'width': 70}),
            html.Br(),
            html.H5('Evaluation Time', style={'display': 'inline-block', 'width': 100}),
            dcc.Input('layering_repeat_evaluation_time', type='text', style={'width': 70, 'marginRight': 10}),
            html.H5('E.g. 23:55:59.996', style={'display': 'inline-block', 'color': 'grey'}),
            html.Br(),
            html.Button(id='instrument_evaluation_time_submit_button', n_clicks=0, children='submit'),
            dcc.Graph(
                id='snap_shoot_of_best_prices',
                figure=go.Figure())
        ])
    ])

    return layering_repeat_page_lay_out


def get_header():
    header = html.Div([
        html.Div([
            html.H2(children='4023: Layering Repeat',
                    style={
                        'textAlign': 'left'
                    })
        ])
    ])
    return header


def get_order_snap_shoot(instrument, evaluation_date, evaluation_time, cache_result=True):

    redis_conn = RedisDatabase().initialize()
    cached_data = redis_conn.hget('layering_repeat_order_snap_shoot_' + evaluation_date.strftime('%Y_%m_%d'), '{}_{}'.format(instrument, evaluation_time))

    if cached_data:
        cached_data = cached_data.decode('utf-8')
        cached_data = json.loads(cached_data)
        graph = go.Figure(cached_data)
        return graph

    graph = order_book_snap_shoot(instrument, evaluation_date, evaluation_time)

    if cache_result:
        graph_dict_obj = graph.to_plotly_json()
        graph_json_obj = json.dumps(graph_dict_obj, cls=ComplexEncoder)
        redis_conn.hset('layering_repeat_order_snap_shoot_' + evaluation_date.strftime('%Y_%m_%d'), '{}_{}'.format(instrument, evaluation_time), graph_json_obj)

        # cache for 7 days
        redis_conn.expire('layering_repeat_order_snap_shoot_' + evaluation_date.strftime('%Y_%m_%d'), 604800)

    return graph


def get_alerts_4023(evaluation_date, cache_result=True):

    redis_conn = RedisDatabase().initialize()
    cached_data = redis_conn.get('layering_repeat_alerts_' + evaluation_date.strftime('%Y_%m_%d'))

    if cached_data:
        cached_data = pd.read_msgpack(cached_data)
        return cached_data

    # all available instruments
    other_condition = 'code = 4023'

    alerts_data_set = (SmartsAlertsDataModel()
                       .initialize(evaluation_date=evaluation_date, other_condition=other_condition)
                       .evaluate()).sort_values('timestamp')

    sub_alerts_data_set = alerts_data_set.groupby('id').last()

    text_result = []
    for index_, row in sub_alerts_data_set.iterrows():
        alert_text = row['longtext'].split('. ')
        alert_text_wrap = [part + '.\n' for part in alert_text[:-1]]
        final_alert_text = ''.join(element for element in alert_text_wrap) + alert_text[-1]
        text_result.append(final_alert_text)

    result = pd.DataFrame({'Alerts on {}'.format(evaluation_date): text_result})

    if cache_result:
        redis_conn.set('layering_repeat_alerts_' + evaluation_date.strftime('%Y_%m_%d'), result.to_msgpack(compress='zlib'))

    return result
