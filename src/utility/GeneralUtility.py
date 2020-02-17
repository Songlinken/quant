import calendar
import datetime
import json
import numpy as np
import random
import string

from functools import wraps


def date_range(begin_date, end_date):
    """
    Get list of dates between two dates (both end inclusive).
    :param begin_date: date
    :param end_date: date
    :return: list of dates
    """
    date_list = []
    while begin_date <= end_date:
        date_list.append(begin_date)
        begin_date += datetime.timedelta(days=1)

    return date_list


def get_first_day_of_last_month():
    """
    Get first day of last month based on current date.
    """
    date_ = datetime.datetime.today()
    year = date_.year
    month = date_.month
    if month == 1:
        month = 12
        year -= 1
    else:
        month -= 1
    return datetime.datetime(year, month, 1)


def get_last_day_of_last_month():
    """
    Get last day of last month based on current date.
    """
    date_ = datetime.datetime.today()
    year = date_.year
    month = date_.month
    if month == 1:
        month = 12
        year -= 1
    else:
        month -= 1
    days = calendar.monthrange(year, month)[1]
    return datetime.datetime(year, month, days)


def random_string(size):
    """
    Get random strings with length of specified size.
    :param size: int
    :return: string
    """
    char_list = string.ascii_lowercase + string.ascii_uppercase + string.digits
    return ''.join(random.choice(char_list) for _ in range(size))


def swap(a, b):
    """
    Swap element position in a list.
    """
    temp = a
    a = b
    b = temp
    return [a, b]


def timer(func):
    """
    Timer decorator for input function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.datetime.now()

        print(func.__name__, end_time - start_time)
        return result
    return wrapper


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, datetime.time):
            return obj.strftime('%H:%M:%S.%f')
        else:
            return json.JSONEncoder.default(self, obj)
