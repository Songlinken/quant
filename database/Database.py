import logging as log
import pymysql

from sys import _getframe
from time import sleep

from utility.Configuration import Configuration


def initialize(connection_config, host='localhost', port=3306):
    """
    Construct a valid database connection with connection_config.
    """
    try:
        db_connection = pymysql.connect(
            host=host,
            port=port,
            database=connection_config['name'],
            user=connection_config['username'],
            password=connection_config['password']
        )

    except ImportError:
        log.error('Failed to load pymysql.')
        raise

    except pymysql.DatabaseError:
        log.error('Failed to connect to database in {} function.').format(_getframe().f_code.co_name)
        raise

    return db_connection


class Database(object):
    """
    Initialize database connection.
    """
    def __init__(self):
        self.connection = None

    def get(self, database, connection_config=None):

        if not connection_config:
            connection_config = Configuration.get()
            dbs = connection_config.get('databases')
            db_exist = next((item for item in dbs if item['name'] == database), None)
            if not db_exist:
                raise Exception('Mysql database configuration of {} is not set properly.'.format(database)) from None

        connection = initialize(db_exist)

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

            except (pymysql.OperationalError, pymysql.InterfaceError) as exception:
                log.error(exception)

        raise Exception('Could not connect to {} database'.format(database))
