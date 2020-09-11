import datetime
import gzip
import json
import pandas as pd
import pprint
import threading

from websocket import WebSocketApp

from src.utility.DataModelUtility import data_frame_to_sql


stream_df = pd.DataFrame(columns=['trade_id', 'event_date', 'event_time', 'symbol', 'side', 'price', 'quantity'])


class HuobiMarketTrade(object):

    def __init__(self, market):
        super(HuobiMarketTrade, self).__init__()
        self.market = market
        self.url = 'wss://api-aws.huobi.pro/ws'
        self.ws = None
        self.stream_df = stream_df
        self.columns = stream_df.columns
        self.table = 'huobi_{}_market_trade '.format(market) + str(self.columns.tolist()).replace('[', '(').replace(']', ')').replace("'", '')

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

        else:
            for trade in msg_dict['tick']['data']:
                event_time = [datetime.datetime.fromtimestamp(msg_dict['ts'] / 1000)]
                event_date = [event_time[0].date()]
                symbol = [msg_dict['ch'].split('.')[1].upper()]
                stream_row = [trade['tradeId']] + event_date + event_time + symbol + [trade['direction']] + [trade['price']] + [trade['amount']]
                self.stream_df = self.stream_df.append(pd.DataFrame([stream_row], columns=self.columns), ignore_index=True)

        if self.stream_df.shape[0] >= 100:
            self.stream_df = self.stream_df.drop_duplicates()
            data_frame_to_sql(self.stream_df, self.table, schema='public', ssh=None, database='quantml', port=5432)
            self.stream_df.drop(self.stream_df.index, inplace=True)
            print('Finished updating table.')

    def on_error(self, error):
        print('Error: ' + str(error))
        error = gzip.decompress(error).decode()
        pprint.pprint(error)

    def on_close(self):
        HuobiMarketTrade(self.market).start()
        print("### closed ###")

    def on_open(self):
        def run(*args):
            data = {
                'sub': 'market.{}usdt.trade.detail'.format(self.market),
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
    HuobiMarketTrade('btc').start()
