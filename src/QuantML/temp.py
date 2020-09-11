import coinbasepro as cbp

from src.utility.GeneralUtility import timer
from temp_2 import temp_client
from temp3 import temp_client_2
from temp4 import temp_client4
import binance
from src.okex_python.okex.spot_api import SpotAPI

binance.set('9mcqwSBQcl7DQgvoWIfhiItRT5cG3Q9jqIZi1C39E71AJ0fqToDsqrfOvsGQN4pC', '5lejNu7PdlOifK0oBOrXwAOkHz69UjEuUvXo5KDzLaCjOJuiahLAceOjTBIidEQQ')
ok_client = SpotAPI('7fbc96aa-c35b-4ec3-b61b-781a027a8305', '4C88AC97DEC60F9C309530BC57E49CCC', 'Maik1990~', use_server_time=True)


class temp():

    @timer
    def arbitrage(self, ok_ex_fee=0.0015, huobi_fee=0.002):
        # coinbase_market = cbp.PublicClient().get_product_order_book(product_id='XRP-USDT')
        ok_ex =temp_client().get_product_order_book('XMR-USDT')
        # binan = temp_client_2().get_product_order_book('XMRUSDT')
        huobi = temp_client4().get_product_order_book('xmrusdt')
        huobi = huobi['tick']

        ok_ex_best_bid = ok_ex['bids'][0]
        ok_ex_best_ask = ok_ex['asks'][0]
        # gateio_best_bid = gateio['bids'][0]
        # gateio_best_ask = gateio['asks'][0]

        huobi_best_bid = huobi['bids'][0]
        huobi_best_ask = huobi['asks'][0]

        short_ok_ex_long_binan_vol = min(float(ok_ex_best_bid[1]), float(huobi_best_ask[1]))
        short_binan_long_ok_ex = min(float(ok_ex_best_ask[1]), float(huobi_best_bid[1]))

        ok_ex_sell_value = float(ok_ex_best_bid[0]) * short_ok_ex_long_binan_vol * (1 - ok_ex_fee)
        huobi_buy_value = float(huobi_best_ask[0]) * short_ok_ex_long_binan_vol * (1 - huobi_fee)

        ok_ex_buy_value = float(ok_ex_best_ask[0]) * short_binan_long_ok_ex * (1 - ok_ex_fee)
        huobi_sell_value = float(huobi_best_bid[0]) * short_binan_long_ok_ex * (1 - huobi_fee)


        if (ok_ex_sell_value - huobi_buy_value) /  ok_ex_sell_value > 0.0012:
            # para = {'pair':'XRP_USD', 'quantity': 3, 'price': exmo_buy_value, 'type':'sell'}
            #
            # action = ExmoAPI().api_query('order_create', para)
            import ipdb;ipdb.set_trace()

        elif (huobi_sell_value - ok_ex_buy_value) / huobi_sell_value > 0.0012:
            import ipdb;ipdb.set_trace()
            result = binance.order('XMRUSDT', 'sell', short_binan_long_ok_ex, float(binan_best_bid[0]), timeInForce='IOC')
            buy_quant = float(result['executedQty'])

            abc = ok_client.take_order(otype='IOC',side='buy',instrument_id='xmr-usdt',size=buy_quant,price=float(ok_ex_best_ask[0]))


if __name__ == '__main__':
    while True:
        result = temp().arbitrage()
