import pandas as pd
import datetime
import quandl
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


quandl.ApiConfig.api_key = 'Jszm-d1BscjHYRVaMmUX'


def obtain_list_of_db_tickers():
    # obtain tickers from database
    with mysql_engine().connection:
        cur = mysql_engine().connection.cursor()
        cur.execute("SELECT id, ticker FROM symbol")
        data = cur.fetchall()
        id_and_symbols = list(data)

        return id_and_symbols


def get_daily_historic_data_quandl(id_and_symbols, start_date='2000-01-01', end_date='2018-01-01'):
    """
    Obtains and parse price data from Quandl.
    """
    data_set = []
    try:
        for id, ticker in id_and_symbols:
            data = quandl.get_table('WIKI/PRICES', ticker=ticker, date={'gte': start_date, 'lte': end_date})
            data['symbol_id'] = id
            data_set.append(data)

    except Exception as e:
        print('Could not download Quandl data: %s' % e)

    data_set = pd.concat(data_set)

    return data_set


def insert_daily_data_into_db(data_set):
    # insert data to daily_price table
    import ipdb;ipdb.set_trace()
    today = datetime.date.today()
    columns_in_db = ['data_vendor_id', 'symbol_id', 'price_date', 'created_date', 'last_updated_date', 'open_price',
                     'high_price', 'low_price', 'close_price', 'adj_close_price', 'volume']

    data_set['data_vendor_id'] = 1
    data_set['created_date'] = today
    data_set['last_updated_date'] = today

    column_name_mapping = {'date': 'price_date',
                           'open': 'open_price',
                           'close': 'close_price',
                           'high': 'high_price',
                           'low': 'low_price',
                           'adj_close': 'adj_close_price'}

    data_set = data_set.rename(columns=column_name_mapping)
    data_set = data_set[columns_in_db]
    data_set.to_sql(con=mysql_engine().engine, name='daily_price', if_exists='append', index=False, chunksize=10000)


if __name__ == '__main__':
    id_and_symbols = obtain_list_of_db_tickers()
    data_set = get_daily_historic_data_quandl(id_and_symbols)
    insert_daily_data_into_db(data_set)