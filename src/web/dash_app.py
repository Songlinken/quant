import dash_core_components as dcc
import dash_html_components as html
import datetime
import flask
import os
import plotly.graph_objs as go

from dash.dependencies import Input, Output, State

from src.web.components.home_page import home_page
from src.web.components.unusual_price_movement_page import unusual_price_movement_page, get_alerts_1001
from src.web.components.layering_repeat_page import layering_repeat_page, get_order_snap_shoot, get_alerts_4023
from src.web.components.wash_sale_aba_m_page import wash_sale_aba_m_page, get_account_daily_activity, get_alerts_4042
from src.web.dash_app_object import app

STATIC_PATH = os.getcwd() + '/src/web/pictures/'

app.layout = html.Div([
    dcc.Location(id='url', refresh=True),
    html.Div(id='page-content')
])


# update page
@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname is '/':
        return home_page()

    elif pathname == '/mktsrv/1001':
        return unusual_price_movement_page()

    elif pathname == '/mktsrv/4023':
        return layering_repeat_page()

    elif pathname == '/mktsrv/4042':
        return wash_sale_aba_m_page()


# Home Page #
# update evaluation_date
@app.callback(Output('output_date_home', 'children'),
              [Input('date_picker_home', 'date')])
def update_evaluation_date(date_):
    return date_


@app.server.route(STATIC_PATH + 'gemini_logo.png')
def server_static():
    return flask.send_from_directory(STATIC_PATH, 'gemini_logo.png')
# Home Page End #


# Unusual Price Movement Intra-day #
@app.callback(Output('output_date_1001', 'children'),
              [Input('date_picker_1001', 'date')])
def update_evaluation_date(date_):
    return date_


@app.callback([Output('alerts_1001', 'data'),
               Output('alerts_1001', 'columns')],
              [Input('output_date_1001', 'children')])
def update_alerts(date_):
    date_ = datetime.datetime.strptime(date_, '%Y-%m-%d').date()
    alerts = get_alerts_1001(date_)
    return alerts.to_dict('rows'), [{'id': c, 'name': c} for c in alerts.columns]
# Unusual Price Movement Intra-day End #


# Layering Repeat #
@app.callback(Output('output_date_4023', 'children'),
              [Input('date_picker_4023', 'date')])
def update_evaluation_date(date_):
    return date_


@app.callback([Output('alerts_4023', 'data'),
               Output('alerts_4023', 'columns')],
              [Input('output_date_4023', 'children')])
def update_alerts(date_):
    date_ = datetime.datetime.strptime(date_, '%Y-%m-%d').date()
    alerts = get_alerts_4023(date_)
    return alerts.to_dict('rows'), [{'id': c, 'name': c} for c in alerts.columns]


@app.callback(Output('snap_shoot_of_best_prices', 'figure'),
              [Input('instrument_evaluation_time_submit_button', 'n_clicks')],
              [State('output_date_4023', 'children'),
               State('layering_repeat_instrument', 'value'),
               State('layering_repeat_evaluation_time', 'value')])
def update_account_daily_activity(n_clicks, date_, instrument, evaluation_time):
    if instrument and evaluation_time is not None:
        date_ = datetime.datetime.strptime(date_, '%Y-%m-%d').date()
        time_ = datetime.datetime.strptime(evaluation_time, '%H:%M:%S.%f').time()
        return get_order_snap_shoot(instrument, date_, time_)
    return go.Figure()
# Layering Repeat End #


# Wash Sale ABA Money Pass Page #
# update evaluation_date
@app.callback(Output('output_date_4042', 'children'),
              [Input('date_picker_4042', 'date')])
def update_evaluation_date(date_):
    return date_


# update alerts on update_wash_sale_aba_m_page
@app.callback([Output('alerts_4042', 'data'),
               Output('alerts_4042', 'columns')],
              [Input('output_date_4042', 'children')])
def update_alerts(date_):
    date_ = datetime.datetime.strptime(date_, '%Y-%m-%d').date()
    alerts = get_alerts_4042(date_)
    return alerts.to_dict('rows'), [{'id': c, 'name': c} for c in alerts.columns]


# update account_daily_activity on update_wash_sale_aba_m_page
@app.callback(Output('account_daily_activity', 'figure'),
              [Input('account_id_instrument_submit_button', 'n_clicks')],
              [State('output_date_4042', 'children'),
               State('wash_sale_account_id', 'value'),
               State('wash_sale_instrument', 'value')])
def update_account_daily_activity(n_clicks, date_, account_id, instrument):
    if account_id is not None and instrument:
        date_ = datetime.datetime.strptime(date_, '%Y-%m-%d').date()
        return get_account_daily_activity(account_id, instrument, date_)
    return go.Figure()
# Wash Sale ABA Money Pass Page End #


if __name__ == '__main__':
    app.run_server(debug=True)
