import datetime
import requests
import MySQLdb as mdb
from bs4 import BeautifulSoup


def obtain_wiki_sp500_symbols():
    """
    Download and parse symbols fo S&P 500 from Wikipedia.
    """
    # stores current time for created_at record
    current_time = datetime.datetime.utcnow()

    # list of S&P 500 companies and obtain their symbol table
    http_response = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = BeautifulSoup(http_response.text)

    symbol_list = soup.select('table')[0].select('tr')[1:]

    symbols = []
    for i, symbol in enumerate(symbol_list):
        tds = symbol.select('td')
        symbols.append(
            (
                tds[0].select('a')[0].text,  # Ticker
                'Stock',
                tds[1].select('a')[0].text,  # Name
                tds[3].text,                 # Sector
                'USD', current_time, current_time
            )
        )

    return symbols


def insert_sp500_symbols(symbols):
    """
    Insert the S&P500 symbols into the MySQL database.
    """
    # connect to the MySQL instance
    db_host = 'localhost'
    db_user = 'sguo'
    db_pass = 'gsl1990~'
    db_name = 'stock'
    connection = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)

    # create the insert strings
    column_str = 'ticker, instrument, name, sector, currency, created_date, last_updated_date'
    insert_str = ("%s, " * 7)[:-2]
    sql = 'INSERT INTO symbol (%s) VALUES (%s)' % (column_str, insert_str)

    # insert symbols to database
    with connection:
        cursor = connection.cursor()
        cursor.executemany(sql, symbols)


if __name__ == '__main__':
    symbols = obtain_wiki_sp500_symbols()
    insert_sp500_symbols(symbols)
    print('%s symbols were successfully added to database table.' % len(symbols))