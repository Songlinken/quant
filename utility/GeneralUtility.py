import datetime
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


def random_string(size):
    """
    Get random strings with length of specified size.

    :param size: int
    :return: string
    """
    char_list = string.ascii_lowercase + string.ascii_uppercase + string.digits
    return ''.join(random.choice(char_list) for _ in range(size))


def timer(func):
    """
    Time decorator for input function.

    :param func: object
    :return: object
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.datetime.now()

        print(func.__name__, end_time - start_time)
        return result
    return wrapper
