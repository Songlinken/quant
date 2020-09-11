import dash_core_components as dcc
import dash_html_components as html
import os

from src.web.components.make_html_calendar import make_html_calendar

STATIC_PATH = os.getcwd() + '/src/web/pictures/'


def home_page():
    return html.Div([
        html.Div([
            get_logo()
        ]),
        html.Div([
            get_header()
        ]),
        html.Div([
            make_html_calendar('home')
        ]),
        html.Div(id='output_date_home', style={'display': 'none'}),
        html.Br(),
        html.Br(),
        html.Div([
            get_alert_types()
        ])
    ])


def get_logo():
    logo = html.Div([
        html.Div([
            html.Img(src=STATIC_PATH + 'gemini_logo.png', height='40', width='180')
        ])
    ])
    return logo


def get_header():
    header = html.Div([
        html.Div([
            html.H1(children='Market Surveillance Alert Review',
                    style={
                        'textAlign': 'center'
                    })
        ])
    ])
    return header


def get_alert_types():
    alert_types = html.Div([
        dcc.Link('1001: Unusual Price Movement Intra-Day', href='/mktsrv/1001'),
        html.Br(),
        html.Br(),
        dcc.Link('2011: Unusual Volume Intra-Day', href='/mktsrv/2011'),
        html.Br(),
        html.Br(),
        dcc.Link('4009: Painting the Tape', href='/mktsrv/4009'),
        html.Br(),
        html.Br(),
        dcc.Link('4011: Crossing at Short Term High/Low', href='/mktsrv/4011'),
        html.Br(),
        html.Br(),
        dcc.Link('4012: Price Driver', href='/mktsrv/4012'),
        html.Br(),
        html.Br(),
        dcc.Link('4022: Bait and Switch', href='/mktsrv/4022'),
        html.Br(),
        html.Br(),
        dcc.Link('4023: Layering Repeat', href='/mktsrv/4023'),
        html.Br(),
        html.Br(),
        dcc.Link('4032: Multi Order Spoofing with Bait and Switch', href='/mktsrv/4032'),
        html.Br(),
        html.Br(),
        dcc.Link('4040: Wash Sales A - A', href='/mktsrv/4040'),
        html.Br(),
        html.Br(),
        dcc.Link('4041: Wash Sales A - B - A', href='/mktsrv/4041'),
        html.Br(),
        html.Br(),
        dcc.Link('4042: Wash Sales A - B - A (Money Pass)', href='/mktsrv/4042'),
        html.Br(),
        html.Br(),
        dcc.Link('4045: Collusion', href='/mktsrv/4045'),
        html.Br(),
        html.Br(),
        dcc.Link('4047: Pre-arranged Trade', href='/mktsrv/4047')
    ])
    return alert_types
