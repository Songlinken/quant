import datetime
import logging as log
import pandas as pd

from database.Database import Database

log.basicConfig(level=log.INFO)


def check_execute_arguments(arguments, keyword_arguments):
    """
    Validate arguments calls to execute function.
    """
    if arguments and keyword_arguments:
        raise Exception('Cannot set both arguments and keyword arguments in execute_query at the same time.')

    if isinstance(arguments, dict):
        log.warning('Arguments should not be a dictionary type, please use keyword arguments.')
        keyword_arguments = arguments
        arguments = None

    return arguments, keyword_arguments


def execute_query_data_frame(query, database, arguments=None, keyword_arguments=None, connection=None, cx_arraysize=None):
    """
    Execute a query and return a pandas dataframe of the query.
    Name the query input arguments in the sql query as their desirable format.

    :param query: string
    :param database: string
    :param arguments: list
    :param keyword_arguments: dict
    :param connection: object
    :param cx_arraysize: int
    :return: pandas dataframe
    """
    arguments, keyword_arguments = check_execute_arguments(arguments, keyword_arguments)
    start_time = datetime.datetime.now()

    try:
        if not connection:
            connection_model = Database()
            connection = connection_model.get(database)
        cursor = connection.cursor()

        # when change cx_arraysize, be aware of memory
        if cx_arraysize:
            cursor.arraysize = cx_arraysize

        if arguments:
            cursor.execute(query, vars=arguments)

        elif keyword_arguments:
            cursor.execute(query, vars=keyword_arguments)

        else:
            cursor.execute(query)

        data_frame = dataframe_from_cursor(cursor)

        log.info('Loaded dataframe with {} rows in {}'.format(data_frame.shape[0], str(datetime.datetime.now() - start_time)))

    except Exception:
        log.error('Failed to execute database query: {}'.format(query))

        if connection:
            connection.rollback()

        raise

    finally:
        connection.close()


def dataframe_from_cursor(cursor):
    """
    Convert a filled up cursor to a pandas dataframe.

    :param cursor: cursor with query executed
    :return: pandas dataframe
    """
    result = cursor.fetchall()

    # if blank result returned, put None instead of an empty list
    if not result:
        return None

    data_frame = pd.DataFrame(data=result, columns=[desc[0].lower() for desc in cursor.description])

    return data_frame
