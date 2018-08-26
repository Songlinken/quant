import pandas as pd
import MySQLdb as mdb
from sqlalchemy import create_engine
from sklearn import covariance, cluster


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
             SELECT dp.price_date, dp.open_price, dp.adj_close_price, s.ticker
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


def sp500_cluster(data_set):
    data_set['diff'] = data_set['adj_close_price'] - data_set['open_price']
    data_set = data_set.pivot(index='price_date', columns='ticker', values='diff')
    data_set = data_set.fillna(0)
    data_set_std = data_set / data_set.std(0)

    edge_model = covariance.GraphLassoCV()
    edge_model.fit(data_set_std)


if __name__ == '__main__':
    data_set = get_data()
    sp500_cluster(data_set)