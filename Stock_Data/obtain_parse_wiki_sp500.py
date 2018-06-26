import datetime
import requests
from bs4 import BeautifulSoup


def obtain_parse_wiki_sp500():
    """
    Download and parse Wikipedia list of S&P 500.
    Return a list of tuples adding to Mysql.
    """

    # Stores current time for created_at record
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