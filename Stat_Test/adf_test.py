import pandas as pd
import MySQLdb as mdb
from statsmodels.tsa.stattools import adfuller
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


def random_stock_test(data_set):
    data_set = data_set.groupby('name').apply(adf_test)
    data_set = data_set.reset_index(drop=True)
    non_random_stock = data_set.loc[data_set['random_walk'] is False, 'name']

    return non_random_stock


def adf_test(data_set, confidence_level='5%'):
    data_set = data_set.sort_values(['price_date'])
    adf_test_result = adfuller(data_set['adj_close_price'], 1)

    critical_value = adf_test_result[0]
    confidence_level_value = adf_test_result[4][confidence_level]

    if critical_value > confidence_level_value:
        data_set['random_walk'] = True

    else:
        data_set['random_walk'] = False

    return data_set


if __name__ == '__main__':
    data = get_data()
    random_stock_test(data)