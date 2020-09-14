import datetime
import logging
import pandas as pd

from binance_d import SubscriptionClient
from binance_d.constant.test import *
from binance_d.model import *
from binance_d.exception.binanceapiexception import BinanceApiException
from src.utility.DataModelUtility import data_frame_to_sql

logger = logging.getLogger("binance-client")
logger.setLevel(level=logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

sub_client = SubscriptionClient(api_key=g_api_key, secret_key=g_secret_key)


def callback(data_type: 'SubscribeMessageType', event: 'any'):
    if data_type == SubscribeMessageType.RESPONSE:
        print("Event ID: ", event)

    elif data_type == SubscribeMessageType.PAYLOAD:

        event_time = datetime.datetime.fromtimestamp(event.eventTime / 1000)
        trade_time = datetime.datetime.fromtimestamp(event.time / 1000)
        symbol = event.symbol
        price = event.price
        quantity = event.qty
        is_buyer_maker = event.isBuyerMaker

        stream_row = [event_time] + [trade_time] + [symbol] + [price] + [quantity] + [is_buyer_maker]
        stream_df = pd.DataFrame([stream_row], columns=['event_time', 'trade_time', 'symbol', 'price', 'quantity', 'is_buyer_maker'])

        table = 'binance_btc_market_trade ' + str(stream_df.columns.tolist()).replace('[', '(').replace(']', ')').replace("'", '')
        data_frame_to_sql(stream_df, table, schema='public', ssh=None, database='quantml', port=5432)
        stream_df.drop(stream_df.index, inplace=True)
        print('Finished updating table.')
        # sub_client.unsubscribe_all()

    else:
        print("Unknown Data:")


def error(e: 'BinanceApiException'):
    print(e.error_code + e.error_message)


sub_client.subscribe_aggregate_trade_event("btcusdt", callback, error)
