import datetime
import io
import logging as log
import pandas as pd
import psycopg2


from sqlalchemy import create_engine

from src.database.Database import Database
from src.database.DatabaseTypeMapping import get_database_to_dataframe_type_map
from src.database.SSHtunnel import SSHManager
from src.utility.Configuration import Configuration

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


def data_delete_from_table(query, ssh='interim', database='gemrdsdb', host='localhost', port=55432):
    """
    Delete data from database table.
    """
    configuration = Configuration.get()
    dbs = configuration.get('databases')
    db_exist = next((item for item in dbs if item["name"] == database), None)
    if not db_exist:
        raise Exception('Postgres database configuration of {} is not set properly.'.format(database))

    if ssh:
        configuration = Configuration.get()
        ssh_tunnels = configuration.get('ssh_tunnel')
        ssh_exist = next((item for item in ssh_tunnels if item["name"] == ssh), None)
        ssh_tunnel = SSHManager(ssh_exist)
        ssh_tunnel.ssh_connect()

    engine = create_engine('postgresql+psycopg2://' + db_exist['username'] + ':' + db_exist['password'] + '@{}:{}/'.format(host, port) + database)
    connection = engine.raw_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        connection.commit()
        log.info('delete completed.')

    except Exception:
        connection.rollback()

    cursor.close()

    if ssh:
        ssh_tunnel.connection.close()


def data_frame_to_sql(data_set, table, schema='ms_dev', ssh='interim', database='gemrdsdb', host='localhost', port=55432):
    """
    Write data frame to database table.
    """
    configuration = Configuration.get()
    dbs = configuration.get('databases')
    db_exist = next((item for item in dbs if item["name"] == database), None)
    if not db_exist:
        raise Exception('Postgres database configuration of {} is not set properly.'.format(database))

    table_exist = schema + '.' + table

    if ssh:
        configuration = Configuration.get()
        ssh_tunnels = configuration.get('ssh_tunnel')
        ssh_exist = next((item for item in ssh_tunnels if item["name"] == ssh), None)
        ssh_tunnel = SSHManager(ssh_exist)
        ssh_tunnel.ssh_connect()

        output = io.StringIO()
        data_set.to_csv(output, index=False, header=False)
        output.getvalue()
        output.seek(0)

        engine = create_engine('postgresql+psycopg2://' + db_exist['username'] + ':' + db_exist['password'] + '@{}:{}/'.format(host, port) + database)
        connection = engine.raw_connection()
        cursor = connection.cursor()
        cursor.copy_expert("COPY {} FROM STDIN WITH CSV".format(table_exist), output)
        connection.commit()
        cursor.close()
        ssh_tunnel.connection.close()

    else:
        output = io.StringIO()
        data_set.to_csv(output, index=False, header=False)
        output.getvalue()
        output.seek(0)

        engine = create_engine('postgresql+psycopg2://' + db_exist['username'] + ':' + db_exist['password'] + '@{}:{}/'.format(host, port) + database)
        connection = engine.raw_connection()
        cursor = connection.cursor()
        cursor.copy_expert("COPY {} FROM STDIN WITH CSV".format(table_exist), output)
        connection.commit()
        cursor.close()


def execute_query_data_frame(query, database, arguments=None, keyword_arguments=None, connection=None, cx_arraysize=None, ssh='datalab', port=55432):
    """
    Execute a query and return a pandas dataframe of the query.
    Name the query input arguments in the sql query as their desirable format.

    :param query: string
    :param database: string
    :param arguments: list
    :param keyword_arguments: dict
    :param connection: object
    :param cx_arraysize: int
    :param use_ssh: bool
    :return pandas dataframe
    """
    ssh_connection = None
    arguments, keyword_arguments = check_execute_arguments(arguments, keyword_arguments)
    start_time = datetime.datetime.now()

    try:
        if not connection:
            connection_model = Database()
            connection, ssh_connection = connection_model.get(database=database, ssh=ssh, port=port)
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
        log.info('Loaded dataframe with {} rows in {}.'.format(data_frame.shape[0], str(datetime.datetime.now() - start_time)))

    except Exception:
        log.error('Failed to execute database query: {}'.format(query))

        if connection:
            connection.rollback()

        raise

    finally:
        if connection and (connection.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_INTRANS):
            connection.commit()
            connection.close()

        if ssh_connection:
            ssh_connection.connection.close()

    return data_frame


def dataframe_from_cursor(cursor):
    """
    Convert a filled up cursor to a pandas dataframe.

    :param cursor: cursor with query executed
    :return pandas dataframe
    """
    result = cursor.fetchall()
    if not result:
        return pd.DataFrame(columns=[desc[0].lower() for desc in cursor.description])

    data_frame = pd.DataFrame(data=result, columns=[desc[0].lower() for desc in cursor.description])

    # ensure database and dataframe data_type consistency
    type_mapping = get_database_to_dataframe_type_map()
    data_types = data_frame.dtypes

    for column in cursor.description:
        column_name = column.name
        column_type = data_types[column_name].name

        # handle mismatch column type
        if type_mapping.get(column.type_code, 'object') != column_type:
            as_type = type_mapping[column.type_code]
            if as_type == 'int64' and data_frame[column_name].isnull().any():
                as_type = 'float64'

            data_frame[column_name] = data_frame[column_name].astype(as_type)

    return data_frame


def data_model_date_range(data_model):
    """
    Decide the date range to be used.
    """
    date_field = getattr(data_model, 'date_field')

    if data_model.arguments.get('evaluation_date', None) and data_model.arguments.get('start_date', None):
        date_condition = "{date_field} between %(start_date)s::date and %(evaluation_date)s::date".format(date_field=date_field)

    elif data_model.arguments.get('evaluation_date', None) and data_model.arguments.get('number_of_days', None):
        date_condition = "{date_field} between %(evaluation_date)s::date - '%(number_of_days)s DAYS'::interval and %(evaluation_date)s::date".format(date_field=date_field)

    elif data_model.arguments.get('evaluation_date', None):
        date_condition = "{date_field} = %(evaluation_date)s::date".format(date_field=date_field)

    elif data_model.arguments.get('start_date', None):
        date_condition = "{date_field} >= %(start_date)s::date".format(date_field=date_field)

    else:
        date_condition = "1=1"

    return date_condition
