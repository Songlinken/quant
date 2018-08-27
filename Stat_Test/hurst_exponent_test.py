import pandas as pd
from numpy import std, subtract, polyfit, sqrt, log
import MySQLdb as mdb
from sqlalchemy import create_engine


# database connection to the MySQL instance
class mysql_engine():
    db_host = 'localhost'
    db_user = 'sguo'
    db_pass = 'gsl1990~'
    db_name = 'stock'
    connection = mdb.connect(db_host, db_user, db_pass, db_name)
    engine = create_engine('mysql+mysqldb://sguo:gsl1990~@localhost:3306/stock?charset=utf8')


def get_data():

    sql = """
             SELECT dp.price_date, dp.adj_close_price, s.ticker, s.name
             FROM daily_price dp
             LEFT JOIN symbol s
             ON dp.symbol_id = s.id
             WHERE dp.price_date
             BETWEEN '2000-01-01' AND '2015-01-01';
          """

    try:
        with mysql_engine().connection:
            data_set = pd.read_sql(sql, con=mysql_engine().connection)

        mysql_engine().connection.close()

    except Exception as e:
        print('Could not get data: %s' % e)

    return data_set


def random_stock_test(data_set, up_threshold=0.6, down_threshold=0.4):
    data_set = data_set.groupby('name').apply(hurst_exponent_test)
    data_set = data_set.reset_index(drop=True)

    data_set_up_threshold = data_set.loc[data_set['hurst_exponent'] > up_threshold,
                                         ['ticker', 'name', 'hurst_exponent']].drop_duplicates()

    data_set_down_threshold = data_set.loc[data_set['hurst_exponent'] > down_threshold,
                                           ['ticker', 'name', 'hurst_exponent']].drop_duplicates()

    return data_set_up_threshold, data_set_down_threshold


def hurst_exponent_test(data_set):
    """Returns the Hurst Exponent of the time series vector ts"""
    data_set = data_set.sort_values(['price_date'])

    # create the range of lag values
    max_split = len(data_set['adj_close_price']) // 2
    lags = range(2, max_split)

    # calculate the array of the variances of the lagged differences
    tau = [sqrt(std(subtract(data_set['adj_close_price'][lag:], data_set['adj_close_price'][:-lag]))) for lag in lags]

    # use a linear fit to estimate the Hurst Exponent
    poly = polyfit(log(lags), log(tau), 1)

    # Hurst Exponent from the polyfit output
    data_set['hurst_exponent'] = poly[0] * 2.0

    return data_set


if __name__ == '__main__':
    data = get_data()
    random_stock_test(data)