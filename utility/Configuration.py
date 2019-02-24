import logging as log
import os
import yaml

from jsonschema import validate

from utility.VirtualenvUtility import get_virtualenv_root

log.basicConfig(level=log.INFO)


class Configuration(object):
    """
    Construct configuration
    """
    CONFIGURATION = None

    def __init__(self):
        log.info('Constructing Singleton')

        # config file in Virtualenv
        yaml_file = os.path.join(get_virtualenv_root(), 'conf', 'quant.yaml')

        # use global config file if no config file in Virtualenv
        if not os.path.exists(yaml_file):
            yaml_file = os.path.join(os.getenv('HOME'), 'quant.yaml')
            log.info('Using global configuration {}'.format(yaml_file))

        with open(yaml_file, 'r') as file_buffer, open('utility/conf.yaml', 'r') as schema:
            config = yaml.load(file_buffer)
            schema = yaml.load(schema)
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
