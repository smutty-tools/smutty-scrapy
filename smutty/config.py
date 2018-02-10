import configparser

from smutty.exceptions import SmuttyException


class ConfigurationFile:

    DEFAULT_CONFIG_FILE = "smutty.conf"

    def __init__(self, file_name=None):
        self._file_name = file_name or self.DEFAULT_CONFIG_FILE
        self._config = configparser.ConfigParser()
        try:
            with open(self._file_name) as config_fileobj:
                self._config.read_file(config_fileobj, source=self._file_name)
        except FileNotFoundError as exception:
            raise SmuttyException("Could not find file: {0}".format(exception))

    def get(self, section, key=None):
        try:
            if key:
                return self._config[section][key]
            else:
                # isolate returned value from original
                return {**self._config[section]}
        except KeyError as exception:
            raise SmuttyException("Problem while reading key {1} for section {2} in configuration file {3} : {0}".format(exception, key, section, self._file_name))

    def get_boolean(self, section, key):
        return self._config.getboolean(section, key)
