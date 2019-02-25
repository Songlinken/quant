from sqlalchemy import create_engine

from database.AlphaVantageData import get_daily_crypto_data
from utility.Configuration import Configuration
from utility.GeneralUtility import timer


@timer
def insert_daily_crypto_data(symbol, market, database, output_format='pandas'):
    """
    Insert daily crypto data to mysql database.
    """
    connection_config = Configuration.get()
    dbs = connection_config.get('databases')
    db_exist = next((item for item in dbs if item['name'] == database), None)
    if not db_exist:
        raise Exception('Mysql database configuration of {} is not set properly.'.format(database)) from None

    user = db_exist['username']
    password = db_exist['password']

    # create engine object
    engine = create_engine('mysql+pymysql://{user}:{password}@localhost:3306/{database}?charset=utf8'
                           .format(user=user, password=password, database=database))

    data = get_daily_crypto_data(symbol, market, output_format)
    data = data[symbol]

    data.to_sql(name='daily_crypto_price',
                con=engine,
                if_exists='append',
                index=False,
                index_label=False,
                chunksize=10000)


if __name__ == '__main__':
    insert_daily_crypto_data('BTC', 'USD', 'engine')
