import logging as log
import psycopg2

from sys import _getframe
from time import sleep

from src.database.SSHtunnel import SSHManager
from src.utility.Configuration import Configuration


def initialize(connection_config, ssh, host='localhost', port=55432):
    """
    Construct a valid database connection with connection_config.
    """
    ssh_tunnel = None

    if ssh:
        ssh_tunnel = SSHManager(ssh)
        ssh_tunnel.ssh_connect()
        port = ssh_tunnel.connection.local_bind_port

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

    return db_connection, ssh_tunnel


class Database(object):
    """
    Initialize database connection based on ssh condition.
    """
    def __init__(self):
        self.connection = None
        self.ssh_tunnel = None

    def get(self, database, ssh=None, port=55432, connection_config=None):

        if not connection_config:
            configuration = Configuration.get()
            dbs = configuration.get('databases')
            db_exist = next((item for item in dbs if item["name"] == database), None)
            if not db_exist:
                raise Exception('Postgres database configuration of {} is not set properly.'.format(database)) from None

            ssh_tunnels = configuration.get('ssh_tunnel')
            ssh_exist = next((item for item in ssh_tunnels if item["name"] == ssh), None)

        connection, ssh_tunnel = initialize(db_exist, ssh_exist, port=port)
        self.ssh_tunnel = ssh_tunnel

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
                    return connection, ssh_tunnel

                # if fails to connect database, try again
                sleep(10)

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exception:
                log.error(exception)

        raise Exception('Could not connect to {} database.'.format(database))
