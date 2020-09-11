import datetime

from src.utility.DataModelUtility import data_delete_from_table
from src.utility.GeneralUtility import timer


@timer
def delete_from_daily_best_prices_split_by_account(evaluation_date, **kwargs):
    """
    Delete daily data from daily_best_prices_split_by_account

    :param evaluation_date: date
    """
    date_condition = evaluation_date
    arguments = kwargs

    if arguments.get('other_condition', None):
        other_condition = arguments.get('other_condition')

    else:
        other_condition = '1=1'

    query = """
            delete
            from     ms_dev.daily_best_prices_split_by_account
            where    event_date = '{date_condition}'::date
            and      {other_condition};
    """.format(date_condition=date_condition, other_condition=other_condition)

    data_delete_from_table(query)


if __name__ == '__main__':

    delete_date = datetime.datetime.today().date() - datetime.timedelta(days=95)
    delete_from_daily_best_prices_split_by_account(delete_date)
