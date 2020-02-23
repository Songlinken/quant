import datetime
import gzip
import json
import pandas as pd
import pprint
import threading

from websocket import WebSocketApp

from src.utility.DataModelUtility import data_frame_to_sql


stream_df = pd.DataFrame(columns=['event_date', 'event_time', 'symbol',
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


class HuobiMarketDepth(object):

    def __init__(self, market):
        super(HuobiMarketDepth, self).__init__()
        self.market = market
        self.url = 'wss://api-aws.huobi.pro/ws'
        self.ws = None
        self.stream_df = stream_df
        self.columns = stream_df.columns
        self.table = 'huobi_{}_market_depth '.format(market) + str(self.columns.tolist()).replace('[', '(').replace(']', ')').replace("'", '')

    def send_message(self, msg_dict):
        data = json.dumps(msg_dict).encode()
        print('Sending Message:')
        pprint.pprint(msg_dict)
        self.ws.send(data)

    def on_message(self, msg):
        unzipped_data = gzip.decompress(msg).decode()
        msg_dict = json.loads(unzipped_data)

        print('Recieved Message:')
        pprint.pprint(msg_dict)
        if 'ping' in msg_dict:
            data = {
                'pong': msg_dict['ping']
            }
            self.send_message(data)

        bid_prices = [price[0] for price in msg_dict['tick']['bids']]
        ask_prices = [price[0] for price in msg_dict['tick']['asks']]
        bid_volumes = [price[1] for price in msg_dict['tick']['bids']]
        ask_volumes = [price[1] for price in msg_dict['tick']['asks']]
        event_time = [datetime.datetime.fromtimestamp(msg_dict['tick']['ts'] / 1000)]
        event_date = [event_time[0].date()]
        symbol = [msg_dict['ch'].split('.')[1].upper()]

        stream_row = event_date + event_time + symbol + ask_prices + ask_volumes + bid_prices + bid_volumes
        self.stream_df = self.stream_df.append(pd.DataFrame([stream_row], columns=self.columns), ignore_index=True)

        if self.stream_df.shape[0] >= 100:
            data_frame_to_sql(self.stream_df, self.table, schema='public', ssh=None, database='quantml', port=5432)
            self.stream_df.drop(self.stream_df.index, inplace=True)
            print('Finished updating table.')

    def on_error(self, error):
        print('Error: ' + str(error))
        error = gzip.decompress(error).decode()
        pprint.pprint(error)

    def on_close(self):
        print("### closed ###")

    def on_open(self):
        def run(*args):
            data = {
                'sub': 'market.{}usdt.depth.step1'.format(self.market),
                'id': 'id_1'
            }

            self.send_message(data)

        t = threading.Thread(target=run, args=())
        t.start()

    def start(self):
        self.ws = WebSocketApp(self.url,
                               on_open=self.on_open,
                               on_message=self.on_message,
                               on_error=self.on_error,
                               on_close=self.on_close)

        self.ws.run_forever()


if __name__ == '__main__':
    HuobiMarketDepth('btc').start()
