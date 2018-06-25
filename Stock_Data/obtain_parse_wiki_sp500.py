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