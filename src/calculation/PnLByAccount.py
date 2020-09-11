import datetime
import json
import numpy as np
import pandas as pd
import plotly.graph_objs as go
import plotly.offline as offplt
import warnings

from collections import defaultdict, deque

from src.calculation.CalculationNode import CalculationNode
from src.data_models.AccountBalanceDataModel import AccountBalanceDataModel
from src.data_models.OrderFillEventDataModel import OrderFillEventDataModel
from src.data_models.PnlQueuesDataModel import PnlQueuesDataModel
from src.data_models.TransferEventDataModel import TransferEventDataModel
from src.utility.GeneralUtility import timer, ComplexEncoder

offplt.offline.init_notebook_mode(connected=True)
warnings.filterwarnings("ignore")


class PnLByAccount(CalculationNode):
    """
    This class designed to calculate intra-day P&L by account split by order book.
    """
    @timer
    def get_data(self, instrument, evaluation_date, initialize_benchmark=False):
        """
        Pull necessary data from interim database. Only applies on one day per time.

        :param instrument: list
        :param evaluation_date: date
        :param initialize_benchmark: bool
        :return: pd dataframe
        """
        currency = list(set([i[:3] for i in instrument] + [i[3:] for i in instrument]))

        if initialize_benchmark:
            account_balance = (AccountBalanceDataModel()
                               .initialize(evaluation_date=evaluation_date,
                                           other_condition="as_of::time = '00:00:00'")
                               .evaluate())

            account_balance = (account_balance.rename({'currency': 'trading_pair',
                                                       'amount': 'quantity',
                                                       'as_of': 'created',
                                                       'account_balance_history_raw_id': 'event_id'}, axis=1))

            account_balance['side'] = 'Balance'

        else:
            account_balance = pd.DataFrame()

        transfer_events = (TransferEventDataModel()
                           .initialize(evaluation_date=evaluation_date,
                                       other_condition='currency in {}'.format(currency)
                                                                       .replace('[', '(')
                                                                       .replace(']', ')'))
                           .evaluate())

        transfer_events = (transfer_events.rename({'tx_time': 'created',
                                                   'currency': 'trading_pair',
                                                   'amount': 'quantity',
                                                   'type': 'side'}, axis=1)
                                          .drop(['is_credit', 'is_advance', 'fee', 'metadata'], axis=1))

        order_fill_events = (OrderFillEventDataModel()
                             .initialize(evaluation_date=evaluation_date,
                                         other_condition='trading_pair in {}'.format(instrument)
                                                                             .replace('[', '(')
                                                                             .replace(']', ')'))
                             .evaluate()
                             .sort_values('order_fill_event_key'))

        # merge data sets
        final_data_set = pd.concat([transfer_events, order_fill_events, account_balance], sort=False)

        return final_data_set

    @timer
    def calculate_pnl_by_account(self, data_set, evaluation_date, order_book, initialize_benchmark=False):
        """
        Calculate p & l by account.

        :param data_set: pd dataframe
        :param evaluation_date: date
        :param order_book: str
        :param initialize_benchmark: bool
        :return: pd dataframe
        """
        data_set = data_set.reset_index()
        data_set = data_set.assign(pnl=np.nan, unpnl=np.nan, queues=None)
        columns = data_set.columns
        data_types = data_set.dtypes
        data_array = data_set.to_numpy()

        total_accounts = data_set['account_id'].unique().tolist()

        trade_manager_dict = {account: TradeManager() for account in total_accounts}
        # get cached queues
        if not initialize_benchmark:

            cached_queues = (PnlQueuesDataModel()
                             .initialize(evaluation_date=evaluation_date - datetime.timedelta(days=1),
                                         other_condition="account_id in {}".format(total_accounts)
                                         .replace('[', '(')
                                         .replace(']', ')')
                                         )
                             .evaluate())

            cached_queues_np = (cached_queues.loc[(cached_queues['order_book'] == order_book)
                                & cached_queues['account_id'].isin(total_accounts)].to_numpy())

            for row_ in range(cached_queues_np.shape[0]):

                if None in cached_queues_np[row_]:
                    continue

                recovered_queues = json.loads(cached_queues_np[row_, 2], object_hook=self.date_hook)

                for queue_ in recovered_queues[order_book]:

                    trade_manager_dict[cached_queues_np[row_, 0]]._open_trades[order_book].append(
                        Trade(queue_['time'],
                              queue_['symbol'],
                              queue_['buying'],
                              queue_['price'],
                              queue_['quantity'])
                    )

        for row in range(data_array.shape[0]):
            # column 6 is side
            if data_array[row, 6] == 'Balance':
                buying = True

            elif data_array[row, 6] == 'Deposit':
                buying = True

            elif data_array[row, 6] == 'buy':
                # column 4 is trading_pair, column -4 is order_book
                if data_array[row, 4] != data_array[row, -4]:
                    # column -6 is front_price, column -5 is rear_price, column 12 is price
                    value = data_array[row, -6] - data_array[row, 12] * data_array[row, -5]

                    if data_array[row, -4][:3] == data_array[row, 4][:3]:
                        # column 2 is account_id
                        trade_manager_dict[data_array[row, 2]]._pnl += value
                        data_array[row, -3] = trade_manager_dict[data_array[row, 2]]._pnl
                        data_array[row, -2] = trade_manager_dict[data_array[row, 2]]._unpnl
                        queues = [record.__dict__ for record in trade_manager_dict[data_array[row, 2]]._open_trades[data_array[row, -4]]]
                        queues_dict = {data_array[row, -4]: queues}
                        queues_dict_json = json.dumps(queues_dict, cls=ComplexEncoder)
                        data_array[row, -1] = queues_dict_json
                        continue
                    else:
                        trade_manager_dict[data_array[row, 2]]._pnl -= value
                        data_array[row, -3] = trade_manager_dict[data_array[row, 2]]._pnl
                        data_array[row, -2] = trade_manager_dict[data_array[row, 2]]._unpnl
                        queues = [record.__dict__ for record in trade_manager_dict[data_array[row, 2]]._open_trades[data_array[row, -4]]]
                        queues_dict = {data_array[row, -4]: queues}
                        queues_dict_json = json.dumps(queues_dict, cls=ComplexEncoder)
                        data_array[row, -1] = queues_dict_json
                        continue

                else:
                    buying = True

            elif data_array[row, 6] == 'Withdrawal':
                buying = False

            elif data_array[row, 6] == 'sell':
                if data_array[row, 4] != data_array[row, -4]:
                    value = data_array[row, -6] - data_array[row, 12] * data_array[row, -5]
                    if data_array[row, -4][:3] == data_array[row, 4][:3]:
                        trade_manager_dict[data_array[row, 2]]._pnl -= value
                        data_array[row, -3] = trade_manager_dict[data_array[row, 2]]._pnl
                        data_array[row, -2] = trade_manager_dict[data_array[row, 2]]._unpnl
                        queues = [record.__dict__ for record in trade_manager_dict[data_array[row, 2]]._open_trades[data_array[row, -4]]]
                        queues_dict = {data_array[row, -4]: queues}
                        queues_dict_json = json.dumps(queues_dict, cls=ComplexEncoder)
                        data_array[row, -1] = queues_dict_json
                        continue
                    else:
                        trade_manager_dict[data_array[row, 2]]._pnl += value
                        data_array[row, -3] = trade_manager_dict[data_array[row, 2]]._pnl
                        data_array[row, -2] = trade_manager_dict[data_array[row, 2]]._unpnl
                        queues = [record.__dict__ for record in trade_manager_dict[data_array[row, 2]]._open_trades[data_array[row, -4]]]
                        queues_dict = {data_array[row, -4]: queues}
                        queues_dict_json = json.dumps(queues_dict, cls=ComplexEncoder)
                        data_array[row, -1] = queues_dict_json
                        continue
                else:
                    buying = False
            else:
                continue

            # column 3 is created
            trade = Trade(data_array[row, 3],
                          # column -4 is order_book
                          data_array[row, -4],
                          buying,
                          # column 12 is price
                          data_array[row, 12],
                          # column 5 is quantity
                          data_array[row, 5])

            trade_manager_dict[data_array[row, 2]].process_trade(trade)
            data_array[row, -3] = trade_manager_dict[data_array[row, 2]]._pnl
            data_array[row, -2] = trade_manager_dict[data_array[row, 2]]._unpnl
            queues = [record.__dict__ for record in trade_manager_dict[data_array[row, 2]]._open_trades[data_array[row, -4]]]
            queues_dict = {data_array[row, -4]: queues}
            queues_dict_json = json.dumps(queues_dict, cls=ComplexEncoder)
            data_array[row, -1] = queues_dict_json

        result_data_set = pd.DataFrame(np.row_stack(data_array), columns=columns)
        result_data_set = result_data_set.astype(data_types)
        result_data_set = result_data_set.set_index('index')
        result_data_set.index.name = None

        return result_data_set

    def date_hook(self, json_dict):
        for (key, value) in json_dict.items():
            if key == 'time':
                json_dict[key] = pd.to_datetime(value)
        return json_dict

    @timer
    def evaluate(self, instrument, evaluation_date, initialize_benchmark=False, plot_pnl=False, account_id_list=None):
        """
        Calculate p & l by account, and plot p & l graph.

        :param instrument: list
        :param evaluation_date: date
        :param initialize_benchmark: bool
        :param plot_pnl: bool
        :param account_id_list: None or list
        :return: pd dataframe
        """
        if not isinstance(instrument, list):
            raise ValueError('instrument need to be a list.')

        data_set = self.get_data(instrument, evaluation_date, initialize_benchmark)
        data_set = data_set.sort_values(['created', 'event_id'])

        if 'Balance' in data_set['side'].unique():
            base_order_books = [base for base in instrument if 'USD' in base]
            initial_order_books_price = data_set.loc[data_set['trading_pair'].isin(base_order_books)].groupby('trading_pair', as_index=False)['price'].first()

            for pair in initial_order_books_price['trading_pair'].tolist():
                data_set.loc[(data_set['side'] == 'Balance') & (data_set['trading_pair'] == pair[:3]), 'price'] = (
                    initial_order_books_price.loc[initial_order_books_price['trading_pair'] == pair, 'price'].values[0])

        # calculate crypto-crypto price
        crypto_pairs = [crypto for crypto in instrument if 'USD' not in crypto]

        temp_data_list = []
        for crypto in crypto_pairs:
            front_order_book = crypto[:3] + 'USD'
            rear_order_book = crypto[3:] + 'USD'
            temp_data_set = data_set.loc[data_set['trading_pair'].isin([front_order_book, crypto])]
            temp_data_set.loc[temp_data_set['trading_pair'] == crypto, 'price'] = np.nan
            temp_data_set['price'] = temp_data_set['price'].ffill().bfill()
            temp_data_set['front_price'] = temp_data_set['price']

            front_data = temp_data_set.loc[temp_data_set['trading_pair'] == crypto, ['account_id', 'event_id', 'front_price']]

            temp_data_set = data_set.loc[data_set['trading_pair'].isin([rear_order_book, crypto])]
            temp_data_set.loc[temp_data_set['trading_pair'] == crypto, 'price'] = np.nan
            temp_data_set['price'] = temp_data_set['price'].ffill().bfill()
            temp_data_set['rear_price'] = temp_data_set['price']

            rear_data = temp_data_set.loc[temp_data_set['trading_pair'] == crypto, ['account_id', 'event_id', 'rear_price']]

            data_to_add = front_data.merge(rear_data, on=['event_id', 'account_id'], how='left')
            temp_data_list.append(data_to_add)

        crypto_crypto_price_data = pd.concat(temp_data_list)
        data_set = data_set.merge(crypto_crypto_price_data, on=['event_id', 'account_id'], how='left')

        # calculate pnl
        base_order_books = [order_book for order_book in instrument if 'USD' in order_book]
        all_pairs = data_set['trading_pair'].value_counts().keys().tolist()

        pnl_result_list = []
        for order_book in base_order_books:
            related_instruments = [pair for pair in all_pairs if order_book[:3] in pair]
            order_book_data = data_set.loc[data_set['trading_pair'].isin(related_instruments)]
            order_book_data.loc[order_book_data['trading_pair'].isin([order_book, order_book[:3]]), 'price'] = (
                order_book_data.loc[order_book_data['trading_pair'].isin([order_book, order_book[:3]]), 'price'].ffill().bfill())
            order_book_data['order_book'] = order_book
            order_book_data = self.calculate_pnl_by_account(order_book_data, evaluation_date, order_book, initialize_benchmark)
            order_book_data['event_date'] = pd.to_datetime(order_book_data['created'].dt.date)
            pnl_result_list.append(order_book_data)

        if plot_pnl:
            if not isinstance(account_id_list, list):
                raise ValueError('account_id_list is needed as a input of list.')

            base_data_set = data_set[['account_id', 'event_id', 'created']]
            layout = go.Layout(title='{}: {} PnL'.format(instrument, evaluation_date),
                               yaxis=go.layout.YAxis(
                                   tickformat='$,2f')
                               )
            fig = go.Figure(layout=layout)
            for account in account_id_list:
                sub_data_set = data_set.loc[data_set['account_id'] == account, ['account_id', 'event_id', 'pnl', 'unpnl']]
                data_set_to_plot = base_data_set.merge(sub_data_set, on=['account_id', 'event_id'], how='left').sort_values('created')

                fig.add_trace(
                    go.Scattergl(x=data_set_to_plot['created'].dt.time,
                                 y=data_set_to_plot['pnl'],
                                 name='{} PnL'.format(account),
                                 mode='markers',
                                 marker={'symbol': 'circle',
                                         'size': 8})
                )

                fig.add_trace(
                    go.Scattergl(x=data_set_to_plot['created'].dt.time,
                                 y=data_set_to_plot['unpnl'],
                                 name='{} Unrealized PnL'.format(account),
                                 mode='markers',
                                 marker={'symbol': 'circle',
                                         'size': 8})
                )

            offplt.plot(fig, filename='{}_{}_pnl.html'.format(instrument, evaluation_date))

        return pnl_result_list


class TradeManager(object):
    def __init__(self):
        self._open_trades = defaultdict(deque)
        self._pnl = 0.0
        self._unpnl = 0.0

    def process_trade(self, trade):
        buy_record = self._open_trades[trade.symbol]

        # if no inventory, just add it
        if len(buy_record) == 0:
            buy_record.append(trade)
            return

        # if inventory exists, all trades must be same way (buy or sell)
        # if new trade is same way, again just add it
        if buy_record[0].buying == trade.buying:
            unpnl = sum([((trade.price - order.price) * order.quantity) for order in buy_record])
            # invert if we shroted
            if not trade.buying:
                unpnl *= -1.0

            unpnl = round(unpnl, 2)
            self._unpnl = unpnl

            buy_record.append(trade)
            return

        # otherwise, consume the trades
        while len(buy_record) > 0 and trade.quantity > 0:
            quantity_traded = min(trade.quantity, buy_record[-1].quantity)

            pnl = quantity_traded * (trade.price - buy_record[-1].price)
            # invert if we shorted
            if trade.buying:
                pnl *= -1.0

            pnl = round(pnl, 2)
            self._pnl += pnl

            trade.quantity -= quantity_traded
            buy_record[-1].quantity -= quantity_traded

            if buy_record[-1].quantity == 0:
                buy_record.pop()

        # if the new trade still has quantity left over
        # then add it
        if trade.quantity > 0:
            buy_record.append(trade)

        if len(buy_record) == 0:
            return

        unpnl = sum([((trade.price - order.price) * order.quantity) for order in buy_record])

        if not buy_record[-1].buying:
            unpnl *= -1.0

        unpnl = round(unpnl, 2)
        self._unpnl = unpnl


class Trade(object):
    def __init__(self, time, symbol, buying, price, quantity):
        self.time = time
        self.symbol = symbol
        self.buying = buying
        self.price = price
        self.quantity = quantity


# sample use case of this class
if __name__ == '__main__':
    current_instruments = ['BCHBTC', 'BCHETH', 'BCHUSD',
                           'BTCUSD',
                           'ETHBTC', 'ETHUSD',
                           'LTCBCH', 'LTCBTC', 'LTCETH', 'LTCUSD',
                           'ZECBCH', 'ZECBTC', 'ZECETH', 'ZECLTC', 'ZECUSD']

    evaluation_date = datetime.date(2019, 10, 1)

    calculation_module = PnLByAccount()
    pnl = calculation_module.evaluate(current_instruments, evaluation_date, initialize_benchmark=True)
