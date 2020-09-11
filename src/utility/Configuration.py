import logging as log
import os
import yaml

from jsonschema import validate

from src.utility.VirtualenvUtility import get_virtualenv_root

log.basicConfig(level=log.INFO)


class Configuration(object):
    """
    Construct configuration.
    """
    CONFIGURATION = None

    def __init__(self):
        log.info('Constructing Singleton')

        # config file in Virtualenv
        yamlfname = os.path.join(get_virtualenv_root(), 'conf', 'quant.yaml')

        # use global config file if no config file in Virtualenv
        if not os.path.exists(yamlfname):
            yamlfname = os.path.join(os.getenv('HOME'), 'quant.yaml')
            log.info('Using global configuration {}.'.format(yamlfname))

        with open(yamlfname, 'r') as file_buffer, open(os.getenv('PYTHONPATH') + '/' + 'src/utility/conf.yaml', 'r') as schema:
            config = yaml.safe_load(file_buffer)
            schema = yaml.safe_load(schema)
            validate(config, schema)

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
