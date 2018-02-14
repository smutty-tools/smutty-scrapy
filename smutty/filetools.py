import hashlib
import logging
import os
import shutil
import tempfile

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


class FinalizedTempFile:

    def __init__(self, final_path, file_mode):
        self.final_path = final_path
        self.file_mode = file_mode
        self._temp_fileobj = None

    def __enter__(self):
        self._temp_fileobj = tempfile.NamedTemporaryFile(mode=self.file_mode, delete=False)
        return self._temp_fileobj

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # close temporary file before doing anything
        if self._temp_fileobj is not None:
            self._temp_fileobj.close()
        # on success (not exiting due to exceptions)
        try:
            if exc_type is None:
                # move temporary file to destination
                self._finalize()
        finally:
            # always cleanup
            self._cleanup()

    def _finalize(self):
        logging.debug("Moving %s to %s", self._temp_fileobj.name, self.final_path)
        shutil.move(self._temp_fileobj.name, self.final_path)
        logging.info("Finalized %s", self.final_path)

    def _cleanup(self):
        if self._temp_fileobj is not None:
            delete_file(self._temp_fileobj.name)


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
    logging.debug("Ensuring that temporary file %s is removed", file_path)
    try:
        file_path.remove()
    except FileNotFoundError:
        if not swallow_exceptions:
            raise
    else:
        logging.debug("Removed temporary file %s", file_path)
