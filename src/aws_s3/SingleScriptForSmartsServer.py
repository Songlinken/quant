import boto3
import datetime
import gzip
import logging as log
import os
import pandas as pd
import psycopg2
import yaml

from sys import _getframe
from time import sleep


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


def execute_query_data_frame(query, database, arguments=None, keyword_arguments=None, connection=None, cx_arraysize=None, port=8032):
    """
    Execute a query and return a pandas dataframe of the query.
    Name the query input arguments in the sql query as their desirable format.

    :param query: string
    :param database: string
    :param arguments: list
    :param keyword_arguments: dict
    :param connection: object
    :param cx_arraysize: int
    :return pandas dataframe
    """
    arguments, keyword_arguments = check_execute_arguments(arguments, keyword_arguments)
    start_time = datetime.datetime.now()

    try:
        if not connection:
            connection_model = Database()
            connection = connection_model.get(database=database, port=port)
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

    return data_frame


def get_database_to_dataframe_type_map(additional_type_mapping=None):
    """
    Map the data type from database to the right data type in pandas dataframe.
    """
    psycopg2_type_tuples = [
        (psycopg2.extensions.FLOAT, 'float64'),
        (psycopg2.extensions.DECIMAL, 'float64'),
        (psycopg2.extensions.DATE, 'datetime64[ns]'),
        (psycopg2.extensions.PYDATETIME, 'datetime64[ns]'),
        (psycopg2.extensions.PYDATETIMETZ, 'datetime64[ns]'),
        (psycopg2.extensions.INTEGER, 'int64'),
        (psycopg2.extensions.LONGINTEGER, 'int64'),
        (psycopg2.extensions.BOOLEAN, 'bool'),
        (psycopg2.extensions.INTERVAL, 'timedelta64[ns]')
    ]

    type_mapping = {}

    for psycopg2_type, pandas_type in psycopg2_type_tuples:
        for type_code in psycopg2_type.values:
            type_mapping[type_code] = pandas_type

    if additional_type_mapping:
        for key, value in additional_type_mapping.item():
            type_mapping[key] = value

    return type_mapping


def initialize(connection_config, host='localhost', port=8032):
    """
    Construct a valid database connection with connection_config.
    """
    try:
        db_connection = psycopg2.connect(
            dbname=connection_config['name'],
            user=connection_config['username'],
            password=connection_config['password'],
            host=host,
            port=port
        )

    except ImportError:
        log.error('Failed to load psycopg2.')
        raise

    except psycopg2.DatabaseError:
        log.error('Failed to connect to database in {} function.'.format(_getframe().f_code.co_name))
        raise

    return db_connection


class Configuration(object):
    """
    Construct configuration.
    """
    CONFIGURATION = None

    def __init__(self):
        log.info('Constructing Singleton')

        yamlfname = os.path.join(os.getenv('HOME'), 'msv.yaml')
        log.info('Using global configuration {}.'.format(yamlfname))

        with open(yamlfname, 'r') as file_buffer:
            config = yaml.safe_load(file_buffer)

        Configuration.CONFIGURATION = config
        log.info('Completed loading configuration.')

    @classmethod
    def get(cls):
        """
        Get configuration singleton.
        """
        if not Configuration.CONFIGURATION:
            Configuration()

        return Configuration.CONFIGURATION


class Database(object):
    """
    Initialize database connection.
    """
    def __init__(self):
        self.connection = None

    def get(self, database, port=8032, connection_config=None):

        if not connection_config:
            configuration = Configuration.get()
            dbs = configuration.get('databases')
            db_exist = next((item for item in dbs if item["name"] == database), None)
            if not db_exist:
                raise Exception('Postgres database configuration of {} is not set properly.'.format(database)) from None

        connection = initialize(db_exist, port=port)

        for i in range(6):
            try:
                # check the connection with database
                cursor = connection.cursor()
                cursor.execute('select 1')
                cursor.close()

                # return class connection if successful
                if connection:
                    log.debug('Successfully connected to {} database.'.format(database))
                    self.connection = connection
                    return connection

                # if fails to connect database, try again
                sleep(10)

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exception:
                log.error(exception)

        raise Exception('Could not connect to {} database.'.format(database))


def update_alerts_to_s3(evaluation_date):

    query = """
            select  transid,
                    id,
                    setname,
                    market,
                    startdate,
                    starttime,
                    starttimestamp,
                    date,
                    time,
                    timestamp,
                    code,
                    securityid,
                    securitycode,
                    houseid,
                    housecode,
                    clientid,
                    clientcode,
                    intensity,
                    extrafolder,
                    viewer,
                    commandline,
                    shorttext,
                    longtext,
                    usercode,
                    attachmentfilename,
                    attachmentcontent,
                    isreissue
            from    geminialert
            where   date = '{date_condition}'::date;
            """.format(date_condition=evaluation_date)

    alerts = execute_query_data_frame(query, 'mplsql')

    alerts_compress_str = alerts.to_csv(compression='gzip', index=False)
    alerts_gzip_file = gzip.compress(bytes(alerts_compress_str, 'utf-8'))

    config = Configuration().get()['aws_s3']
    access_key_id = [key_id['access_key_id'] for key_id in config if list(key_id.keys())[0] == 'access_key_id'][0]
    secret_access_key = [secret_key['secret_access_key'] for secret_key in config if list(secret_key.keys())[0] == 'secret_access_key'][0]
    bucket = [bucket['bucket_name'] for bucket in config if list(bucket.keys())[0] == 'bucket_name'][0]

    session = boto3.Session(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
    s3 = session.client('s3')

    s3.put_object(Body=alerts_gzip_file, Key='smarts_alerts_{}.csv.gz'.format(evaluation_date), Bucket=bucket)


if __name__ == '__main__':
    update_alerts_to_s3(datetime.date.today() - datetime.timedelta(days=1))
