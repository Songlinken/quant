import redis

from src.database.SSHtunnel import SSHManager
from src.utility.Configuration import Configuration


class RedisDatabase(object):
    """
    Initialize redis database connection.
    """
    def initialize(self, ssh=None, port=6379, connection_config=None):

        if not connection_config:
            configuration = Configuration.get()
            db_password = configuration.get('redis')

            if not db_password:
                raise Exception('Redis database configuration of is not set properly, missing password.') from None

            ssh_tunnels = configuration.get('ssh_tunnel')
            ssh_exist = next((item for item in ssh_tunnels if item["name"] == ssh), None)

        if ssh:
            ssh_tunnel = SSHManager(ssh_exist)
            ssh_tunnel.ssh_connect()

        redis_db = redis.Redis(host='localhost', port=port, db=0, password=db_password[0]['password'])

        return redis_db
