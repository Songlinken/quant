import dash_core_components as dcc
import dash_html_components as html
import dash_table as dst
import json
import pandas as pd
import plotly.graph_objs as go

from src.alert_analysis.WashSaleAnalysis import WashSaleAnalysis
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.redis.RedisDataBase import RedisDatabase
from src.alert_analysis.SpoofingAnalysis import show_account_daily_action
from src.utility.GeneralUtility import ComplexEncoder
from src.web.components.make_html_calendar import make_html_calendar


def wash_sale_aba_m_page():

    wash_sale_aba_m_page_lay_out = html.Div([

        # header
        html.Div([
            get_header()
        ]),

        # div 1: description
        html.Div([
            html.H4('Wash Sales A - B - A (Money Pass)'),
            html.P("""The Wash Sales (A to B to A) Alert identifies a series of trades which when aggregated,
                      appear to be a wash sale. A wash sale under this alert would be the sequence of participant
                      A buying 1,000 shares from participant B and then selling all 1,000 shares back to B.
                      if the trade is done at the same price, it is a “Wash Sale”.
                   """),
            html.P("""There is a variation of this alert, where if the two trades are done at a different price
                      then the alert is called a “Money Pass”. Effectively one party has passed money to the other
                      party through trade price differentials.
                   """)
        ]),
        html.Br(),

        # calendar
        html.Div([
           make_html_calendar(4042)
        ]),
        html.Div(id='output_date_4042', style={'display': 'none'}),
        html.Br(),

        # div 2: alerts from validation_model
        html.Div([
            html.H4('Alerts Detail'),
            dst.DataTable(
                id='alerts_4042',
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
            html.H4('Trading Activity of Account'),
            html.H5('Account ID', style={'display': 'inline-block', 'width': 70, 'margin': 0, 'padding': 0, 'border': 0}),
            dcc.Input('wash_sale_account_id', type='number', style={'width': 70}),
            html.Br(),
            html.H5('Instrument', style={'display': 'inline-block', 'width': 70}),
            dcc.Input('wash_sale_instrument', type='text', style={'width': 70}),
            html.Br(),
            html.Button(id='account_id_instrument_submit_button', n_clicks=0, children='submit'),
            dcc.Graph(
                id='account_daily_activity',
                figure=go.Figure())
        ])
    ])

    return wash_sale_aba_m_page_lay_out


def get_header():
    header = html.Div([
        html.Div([
            html.H2(children='4042: Wash Sales A - B - A (Money Pass)',
                    style={
                        'textAlign': 'left'
                    })
        ])
    ])
    return header


def get_account_daily_activity(account_id, instrument, evaluation_date, cache_result=True):

    redis_conn = RedisDatabase().initialize()
    cached_data = redis_conn.hget('wash_sale_account_daily_activity_' + evaluation_date.strftime('%Y_%m_%d'), '{}_{}'.format(instrument, account_id))

    if cached_data:
        cached_data = cached_data.decode('utf-8')
        cached_data = json.loads(cached_data)
        graph = go.Figure(cached_data)
        return graph

    graph = show_account_daily_action(account_id, instrument, evaluation_date)

    if cache_result:
        graph_dict_obj = graph.to_plotly_json()
        graph_json_obj = json.dumps(graph_dict_obj, cls=ComplexEncoder)
        redis_conn.hset('wash_sale_account_daily_activity_' + evaluation_date.strftime('%Y_%m_%d'), '{}_{}'.format(instrument, account_id), graph_json_obj)

        # cache for 7 days
        redis_conn.expire('wash_sale_account_daily_activity_' + evaluation_date.strftime('%Y_%m_%d'), 604800)

    return graph


def get_alerts_4042(evaluation_date, stored_type='c', release_version=1, cache_result=True):

    redis_conn = RedisDatabase().initialize()
    cached_data = redis_conn.get('wash_sale_aba_m_alerts_' + evaluation_date.strftime('%Y_%m_%d'))

    if cached_data:
        cached_data = pd.read_msgpack(cached_data)
        return cached_data

    # all available instruments
    instruments = CryptoPairsDataModel().evaluate()['trading_pair'].tolist()
    analysis_model = WashSaleAnalysis()
    model_alerts = analysis_model.get_model_validation_alerts(instruments, stored_type, release_version, evaluation_date, evaluation_date)
    alerts_data_set = pd.concat(model_alerts).sort_values(['date_time', 'side']).reset_index(drop=True)

    # alert report
    alerts_array = alerts_data_set.to_numpy()
    text_result = []
    for row in range(0, alerts_array.shape[0], 2):
        # column 0 is account_id & column 29 is counter_party_account_id & column 31 is trade_time_range
        text_part_1 = ('Wash Sale A-B-A (Money Pass): Broker {account_id} and Broker {counter_party_account_id} have executed an ABA wash sale with a time difference of {trade_time_range}:'
                       .format(account_id=alerts_array[row, 0],
                               counter_party_account_id=alerts_array[row, 29],
                               trade_time_range=alerts_array[row, 31]))

        # column 5 is event_time & column 17 is fill_quantity & column 12 is symbol
        # column 16 is fill_price & column 28 is trade_value & column 11 is side
        # column 29 is counter_party_account_id
        text_part_2 = ('{date_time}: Traded x{fill_quantity} {symbol} at ${fill_price} with a value of {trade_value}, Broker {account_id}({side}) Broker {counter_party_account_id}({other_side})'
                       .format(date_time=alerts_array[row, 2],
                               fill_quantity=alerts_array[row, 17],
                               symbol=alerts_array[row, 12],
                               fill_price=alerts_array[row, 16],
                               trade_value=alerts_array[row, 28],
                               account_id=alerts_array[row, 0],
                               side=alerts_array[row, 11],
                               counter_party_account_id=alerts_array[row, 29],
                               other_side=alerts_array[row + 1, 11]))

        # column 30 is trade_back_time & column 32 is trade_back_price
        text_part_3 = ('{trade_back_time}: Traded x{fill_quantity} {symbol} at ${trade_back_price} with a value of {trade_back_value}, Broker {counter_party_account_id}({side}) Broker {account_id}({other_side})'
                       .format(trade_back_time=alerts_array[row, 30],
                               fill_quantity=alerts_array[row, 17],
                               symbol=alerts_array[row, 12],
                               trade_back_price=alerts_array[row, 32],
                               trade_back_value=alerts_array[row, 32] * alerts_array[row, 17],
                               counter_party_account_id=alerts_array[row, 29],
                               side=alerts_array[row, 11],
                               account_id=alerts_array[row, 0],
                               other_side=alerts_array[row + 1, 11]))

        text_content = text_part_1 + '\n' + text_part_2 + '\n' + text_part_3
        text_result.append(text_content)

    result = pd.DataFrame({'Alerts on {}'.format(evaluation_date): text_result})

    if cache_result:
        redis_conn.set('wash_sale_aba_m_alerts_' + evaluation_date.strftime('%Y_%m_%d'), result.to_msgpack(compress='zlib'))

    return result
