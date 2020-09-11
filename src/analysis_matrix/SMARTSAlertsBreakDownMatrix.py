import datetime
import os
import pandas as pd

from src.analysis_matrix.MarketMakerAndLiquidity import MarketMakerAndLiquidity
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.data_models.SmartsAlertsDataModel import SmartsAlertsDataModel
from src.utility.GeneralUtility import timer


@timer
def smarts_alerts_breakdown_matrix(evaluation_date, begin_date, maker_threshold=0.6, liquidity_volume_threshold=1000, liquidity_value_threshold=100000, to_csv=True):
    """
    SMARTS alerts breakdown matrx. Does not include alert type 4041 & 4042 due to unknown account_id from database.
    Only works for the data within same year currently.

    :param evaluation_date: date
    :param begin_date: date
    :param maker_threshold: float (0 to 1)
    :param liquidity_volume_threshold: float
    :param liquidity_value_threshold: float
    :param to_csv: bool
    """
    home = os.path.expanduser('~')

    def mm_vs_nmm_matrix_support(date_set):
        market_trades_count = date_set.loc[date_set['maker_account'], 'account_id'].count()
        non_market_trades_count = date_set.loc[~date_set['maker_account'], 'account_id'].count()

        date_set.loc[date_set['maker_account'], 'mm_vs_nmm_alert_ratio'] = market_trades_count / (market_trades_count + non_market_trades_count)

        date_set.loc[~date_set['maker_account'], 'mm_vs_nmm_alert_ratio'] = non_market_trades_count / (market_trades_count + non_market_trades_count)

        return date_set

    def liquidity_matrix_support(date_set):
        liquid_trades_count = date_set.loc[date_set['liquid_market'], 'value_counts'].sum()
        non_liquid_trades_count = date_set.loc[~date_set['liquid_market'], 'value_counts'].sum()

        date_set.loc[date_set['liquid_market'], 'liquidity_alert_ratio'] = liquid_trades_count / (liquid_trades_count + non_liquid_trades_count)

        date_set.loc[~date_set['liquid_market'], 'liquidity_alert_ratio'] = non_liquid_trades_count / (liquid_trades_count + non_liquid_trades_count)

        return date_set

    alerts = (SmartsAlertsDataModel()
              .initialize(start_date=begin_date, evaluation_date=evaluation_date)
              .evaluate())
    alerts['housecode'] = alerts['housecode'].astype(float)

    instruments = CryptoPairsDataModel().evaluate()['trading_pair'].tolist()
    data_set_list = []
    for instrument in instruments:
        raw_data = MarketMakerAndLiquidity().get_data(instrument, evaluation_date, begin_date)
        data_set_list.append(raw_data)

    market_maker_data = pd.concat(data_set_list)

    # market maker vs non-market maker
    market_maker_trade_percentage_data = MarketMakerAndLiquidity().market_maker_trade_percentage(market_maker_data)
    final_data_set = market_maker_trade_percentage_data.merge(alerts, left_on='account_id', right_on='housecode', how='left')
    final_data_set = final_data_set.loc[final_data_set['liquidity_indicator'] == 'maker']
    final_data_set['alert_year'] = final_data_set['date'].dt.year
    final_data_set['alert_month'] = final_data_set['date'].dt.month
    final_data_set.loc[final_data_set['market_maker_trade_ratio'] >= maker_threshold, 'maker_account'] = True
    final_data_set.loc[final_data_set['market_maker_trade_ratio'] < maker_threshold, 'maker_account'] = False

    mm_vs_nmm_matrix_percentage = final_data_set.groupby(['code', 'alert_year', 'alert_month']).apply(mm_vs_nmm_matrix_support)
    mm_vs_nmm_matrix_pivot = mm_vs_nmm_matrix_percentage.pivot_table(index=['maker_account', 'alert_year', 'alert_month'],
                                                                     columns='code',
                                                                     values='mm_vs_nmm_alert_ratio')

    mm_vs_nmm_matrix_pivot = round(mm_vs_nmm_matrix_pivot * 100, 2).fillna(0).astype(str) + ' %'

    mm_vs_nmm_matrix = final_data_set.groupby(['maker_account', 'code', 'alert_year', 'alert_month'], as_index=False)['account_id'].count()
    mm_vs_nmm_pivot = (mm_vs_nmm_matrix.pivot_table(index=['maker_account', 'alert_year', 'alert_month'],
                                                    columns='code',
                                                    values='account_id')
                       .fillna(0))

    final_mm_vs_nmm_pivot = mm_vs_nmm_pivot.merge(mm_vs_nmm_matrix_pivot,
                                                  how='left',
                                                  on=['maker_account', 'alert_year', 'alert_month'],
                                                  suffixes={'', ' in %'})

    if to_csv:
        final_mm_vs_nmm_pivot.to_csv(home + '/Downloads/mm_vs_nmm_matrix_pivot.csv')

    # liquid vs non-liquid (quantity)
    trade_volume_by_order_book = (market_maker_data
                                  .groupby(['trading_pair', 'event_month'], as_index=False)
                                  .agg({'event_day': 'nunique', 'quantity': 'sum'})
                                  .rename(columns={'event_day': 'count_day_in_month', 'quantity': 'total_volume'}))

    trade_volume_by_order_book['avg_daily_trade_volume'] = trade_volume_by_order_book['total_volume'] / trade_volume_by_order_book['count_day_in_month'] / 2
    trade_volume_matrix = trade_volume_by_order_book.groupby('trading_pair', as_index=False)['avg_daily_trade_volume'].mean().round(2)
    trade_volume_matrix.loc[trade_volume_matrix['avg_daily_trade_volume'] >= liquidity_volume_threshold, 'liquid_market'] = True
    trade_volume_matrix.loc[trade_volume_matrix['avg_daily_trade_volume'] < liquidity_volume_threshold, 'liquid_market'] = False

    final_trade_volume_matrix = trade_volume_matrix.merge(alerts, left_on='trading_pair', right_on='securitycode', how='left')
    final_trade_volume_matrix = (final_trade_volume_matrix
                                 .groupby(['trading_pair', 'liquid_market', 'code'], as_index=False)['housecode']
                                 .count()
                                 .rename(columns={'housecode': 'value_counts'}))

    trade_volume_matrix_pivot = (final_trade_volume_matrix.pivot_table(index=['liquid_market', 'trading_pair'],
                                                                       columns='code',
                                                                       values='value_counts')
                                 .fillna(0))

    trade_volume_matrix_percentage = final_trade_volume_matrix.groupby('code').apply(liquidity_matrix_support)
    trade_volume_matrix_percentage_pivot = trade_volume_matrix_percentage.pivot_table(index=['liquid_market', 'trading_pair'],
                                                                                      columns='code',
                                                                                      values='liquidity_alert_ratio')

    trade_volume_matrix_percentage_pivot = round(trade_volume_matrix_percentage_pivot * 100, 2).fillna(0).astype(str) + ' %'

    final_trade_volume_pivot = trade_volume_matrix_pivot.merge(trade_volume_matrix_percentage_pivot,
                                                               how='left',
                                                               on=['liquid_market', 'trading_pair'],
                                                               suffixes={'', ' in %'})

    if to_csv:
        final_trade_volume_pivot.to_csv(home + '/Downloads/liquid_vs_non_liquid_(quantity)_matrix_pivot.csv')

    # liquid vs non-liquid (dollar value)
    trade_price_by_order_book = (market_maker_data
                                 .groupby(['trading_pair', 'event_month'], as_index=False)['price']
                                 .mean()
                                 .rename(columns={'price': 'avg_price'}))

    trade_price_by_order_book = trade_price_by_order_book.merge(trade_volume_by_order_book, on=['trading_pair', 'event_month'])
    trade_price_by_order_book['avg_daily_trade_value'] = trade_price_by_order_book['avg_price'] * trade_price_by_order_book['avg_daily_trade_volume']

    # adjust crypto-to-crypto value to dollar value
    for pair in ['BCHBTC', 'BCHETH', 'ETHBTC', 'LTCBCH', 'LTCBTC', 'LTCETH', 'ZECBCH', 'ZECBTC', 'ZECETH', 'ZECLTC']:
        for index_, row in trade_price_by_order_book.iterrows():
            if row['trading_pair'] == pair:
                base_crypto_value_pair = pair[:3] + 'USD'
                base_crypto_value = trade_price_by_order_book.loc[(trade_price_by_order_book['trading_pair'] == base_crypto_value_pair)
                                                                  & (trade_price_by_order_book['event_month'] == row['event_month']), 'avg_price'].values[0]

                trade_price_by_order_book.loc[index_, 'avg_daily_trade_value'] = row['avg_daily_trade_value'] * base_crypto_value

    trade_value_matrix = trade_price_by_order_book.groupby('trading_pair', as_index=False)['avg_daily_trade_value'].mean().round(2)
    trade_value_matrix.loc[trade_value_matrix['avg_daily_trade_value'] >= liquidity_value_threshold, 'liquid_market'] = True
    trade_value_matrix.loc[trade_value_matrix['avg_daily_trade_value'] < liquidity_value_threshold, 'liquid_market'] = False

    final_trade_value_matrix = trade_value_matrix.merge(alerts, left_on='trading_pair', right_on='securitycode', how='left')
    final_trade_value_matrix = (final_trade_value_matrix
                                .groupby(['trading_pair', 'liquid_market', 'code'], as_index=False)['housecode']
                                .count()
                                .rename(columns={'housecode': 'value_counts'}))

    trade_value_matrix_pivot = (final_trade_value_matrix.pivot_table(index=['liquid_market', 'trading_pair'],
                                                                     columns='code',
                                                                     values='value_counts')
                                .fillna(0))

    trade_value_matrix_percentage = final_trade_value_matrix.groupby('code').apply(liquidity_matrix_support)
    trade_value_matrix_percentage_pivot = trade_value_matrix_percentage.pivot_table(index=['liquid_market', 'trading_pair'],
                                                                                    columns='code',
                                                                                    values='liquidity_alert_ratio')

    trade_value_matrix_percentage_pivot = round(trade_value_matrix_percentage_pivot * 100, 2).fillna(0).astype(str) + ' %'

    final_trade_value_pivot = trade_value_matrix_pivot.merge(trade_value_matrix_percentage_pivot,
                                                             how='left',
                                                             on=['liquid_market', 'trading_pair'],
                                                             suffixes={'', ' in %'})

    if to_csv:
        final_trade_value_pivot.to_csv(home + '/Downloads/liquid_vs_non_liquid_(trade_value)_matrix_pivot.csv')

    return final_mm_vs_nmm_pivot, final_trade_volume_pivot, final_trade_value_pivot


# sample use case
if __name__ == '__main__':
    smarts_alerts_breakdown_matrix(datetime.date(2019, 8, 31), datetime.date(2019, 1, 1))
