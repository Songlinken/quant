import logging as log

from sshtunnel import SSHTunnelForwarder

from src.utility.Configuration import Configuration
from src.utility.GeneralUtility import timer


def initialize(connection_string):
    """
    Construct a valid ssh connection.
    """
    try:
        ssh_tunnel = SSHTunnelForwarder(
            ssh_address_or_host=eval(connection_string['host']),
            ssh_username=connection_string['user'],
            ssh_pkey=connection_string['ssh_key'],
            remote_bind_address=eval(connection_string['remote_bind_address']),
            local_bind_address=eval(connection_string['local_bind_address'])
        )

    except ImportError:
        log.error('Failed to load SSHTunnelForwarder.')

    return ssh_tunnel


class SSHManager(object):
    """
    Class for setting up SSH tunnel.
    """
    def __init__(self, connection_detail=None):
        if not connection_detail:
            connection_detail = Configuration.get().get('ssh_tunnel')
        self.connection = initialize(connection_detail)

    @timer
    def ssh_connect(self):
        try:
            self.connection.start()

        except Exception:
            raise RuntimeError('Failed to open SSH tunnel.')
