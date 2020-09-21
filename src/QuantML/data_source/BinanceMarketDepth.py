import datetime
import logging
import pandas as pd

from binance_d import SubscriptionClient
from binance_d.constant.test import *
from binance_d.exception.binanceapiexception import BinanceApiException
from binance_d.model import *
from src.utility.DataModelUtility import data_frame_to_sql

logger = logging.getLogger("binance-futures")
logger.setLevel(level=logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

sub_client = SubscriptionClient(api_key=g_api_key, secret_key=g_secret_key)


def callback(data_type: 'SubscribeMessageType', event: 'any'):

    if data_type == SubscribeMessageType.RESPONSE:
        print("Event ID: ", event)

    elif data_type == SubscribeMessageType.PAYLOAD:

        event_id = event.lastUpdateId
        event_time = datetime.datetime.fromtimestamp(event.eventTime / 1000)
        trade_time = datetime.datetime.fromtimestamp(event.transactionTime / 1000)
        symbol = event.symbol
        bid_prices = [price.price for price in event.bids]
        ask_prices = [price.price for price in event.asks]
        bid_volumes = [volume.qty for volume in event.bids]
        ask_volumes = [volume.qty for volume in event.asks]

        stream_row = [event_id] + [event_time] + [trade_time] + [symbol] + ask_prices + ask_volumes + bid_prices + bid_volumes
        stream_df = pd.DataFrame([stream_row], columns=['event_id', 'event_time', 'trade_time', 'symbol',
                                                        'ask_price_1', 'ask_price_2', 'ask_price_3', 'ask_price_4', 'ask_price_5',
                                                        'ask_price_6', 'ask_price_7', 'ask_price_8', 'ask_price_9', 'ask_price_10',
                                                        'ask_price_11', 'ask_price_12', 'ask_price_13', 'ask_price_14', 'ask_price_15',
                                                        'ask_price_16', 'ask_price_17', 'ask_price_18', 'ask_price_19', 'ask_price_20',
                                                        'ask_volume_1', 'ask_volume_2', 'ask_volume_3', 'ask_volume_4', 'ask_volume_5',
                                                        'ask_volume_6', 'ask_volume_7', 'ask_volume_8', 'ask_volume_9', 'ask_volume_10',
                                                        'ask_volume_11', 'ask_volume_12', 'ask_volume_13', 'ask_volume_14', 'ask_volume_15',
                                                        'ask_volume_16', 'ask_volume_17', 'ask_volume_18', 'ask_volume_19', 'ask_volume_20',
                                                        'bid_price_1', 'bid_price_2', 'bid_price_3', 'bid_price_4', 'bid_price_5',
                                                        'bid_price_6', 'bid_price_7', 'bid_price_8', 'bid_price_9', 'bid_price_10',
                                                        'bid_price_11', 'bid_price_12', 'bid_price_13', 'bid_price_14', 'bid_price_15',
                                                        'bid_price_16', 'bid_price_17', 'bid_price_18', 'bid_price_19', 'bid_price_20',
                                                        'bid_volume_1', 'bid_volume_2', 'bid_volume_3', 'bid_volume_4', 'bid_volume_5',
                                                        'bid_volume_6', 'bid_volume_7', 'bid_volume_8', 'bid_volume_9', 'bid_volume_10',
                                                        'bid_volume_11', 'bid_volume_12', 'bid_volume_13', 'bid_volume_14', 'bid_volume_15',
                                                        'bid_volume_16', 'bid_volume_17', 'bid_volume_18', 'bid_volume_19', 'bid_volume_20'])

        table = 'binance_btc_market_depth ' + str(stream_df.columns.tolist()).replace('[', '(').replace(']', ')').replace("'", '')
        data_frame_to_sql(stream_df, table, schema='public', ssh=None, database='quantml', port=5432)
        stream_df.drop(stream_df.index, inplace=True)
        print('Finished updating table.')

    else:
        print("Unknown Data:")


def error(e: 'BinanceApiException'):
    print(e.error_code + e.error_message)


# Valid limit values are 5, 10, or 20
sub_client.subscribe_book_depth_event("btcusdt", 20, callback, error, update_time=UpdateTime.REALTIME)
# sub_client.subscribe_book_depth_event("btcusd_200925", 10, callback, error, update_time=UpdateTime.NORMAL)
# sub_client.subscribe_book_depth_event("btcusd_200925", 10, callback, error)
