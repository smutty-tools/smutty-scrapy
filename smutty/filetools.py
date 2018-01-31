import logging
import os


class IntegerStateFile:

    def __init__(self, file_name, logger=None):
        self.file_name = file_name
        self.logger = logger or logging.getLogger('')

    def set(self, value):
        if value is None:
            self.logger.debug("Setting value to None deletes state file %s", self.file_name)
            self.delete()
            return

        with open(self.file_name, "wt") as file_obj:
            self.logger.debug("Setting state file %s value to %d", self.file_name, value)
            file_obj.write("{0}".format(value))

    def get(self):
        try:
            with open(self.file_name, "rt") as file_obj:
                value = int(file_obj.read())
                self.logger.debug("Getting state file %s value of %d", self.file_name, value)
                return value
        except FileNotFoundError as exception:
            return None

    def delete(self):
        self.logger.debug("Deleting state file %s", self.file_name)
        os.remove(self.file_name)
