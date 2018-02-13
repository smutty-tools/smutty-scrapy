import hashlib
import logging
import os

from path import Path


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


class OutputDirectory:

    def __init__(self, destination_directory):
        self._destination_directory = Path(destination_directory).expand().abspath()
        self._destination_directory.mkdir_p()

    def __repr__(self):
        return self._destination_directory

    @property
    def path(self):
        return self._destination_directory


def md5_file(file_path):
    hasher = hashlib.md5()
    block_size = 4*1024
    with open(file_path, 'rb', buffering=0) as file_obj:
        while True:
            block = file_obj.read(block_size)
            if not block:
                break
            hasher.update(block)
    result = hasher.hexdigest()
    return result


def delete_file(file_name, swallow_exceptions=True):
    file_path = Path(file_name).expand().abspath()
    logging.debug("Deleting %s", file_path)
    try:
        file_path.remove()
        file_path.remove()
    except FileNotFoundError:
        if not swallow_exceptions:
            raise
    else:
        logging.debug("Removed file %s", file_path)
