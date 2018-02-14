import json
import logging
import lzma
import re
import shutil
import tempfile

from smutty.filetools import delete_file


class Indexer:

    INDEX_NAME = "index"

    PACKAGE_PATTERN = r"^(?P<content_type>video|image)-(?P<min_id>[0-9]+)-(?P<max_id>[0-9]+)-(?P<hash_digest>[0-9a-f]+)\b"

    def __init__(self, destination_directory):
        self._destination_directory = destination_directory
        self._package_info = []

    def __repr__(self):
        return "{0}({_destination_directory})".format(self.__class__.__name__, **self.__dict__)

    @classmethod
    def package_pattern(cls):
        """
        Re-implementation required in sub-classes
        """
        return cls.PACKAGE_PATTERN

    @classmethod
    def index_file_name(cls):
        """
        Re-implementation required in sub-classes
        """
        return cls.INDEX_NAME

    def serialize_index(self, file_obj):
        """
        Implementation required in sub-classes
        """
        raise NotImplementedError()

    def build_package_info(self):
        # build package info
        self._package_info = []
        pattern = re.compile(self.package_pattern())
        for package_file in self._destination_directory.path.files():
            match = pattern.match(package_file.name)
            if not match:
                logging.warning("Invalid package name %s, ignoring", package_file)
                continue
            info = {
                name: match.group(name)
                for name in ['content_type', 'min_id', 'max_id', 'hash_digest']
            }
            self._package_info.append(info)
        # sort entries according to hash (so that exporter runs are stable)
        self._package_info.sort(key=lambda x: x['hash_digest'])
        logging.info("%s packages found", len(self._package_info))

    def serialize_info(self, file_obj):
        """
        Provide a default pass-through implementation
        """
        self.serialize_index(file_obj)

    def remove_existing_index_files(self):
        pattern = "{0}*".format(self.INDEX_NAME)
        for file in self._destination_directory.path.files(pattern):
            logging.debug("Deleting present index file %s", file)
            file.remove()

    def serialize(self):
        """
        Serialize to a temporary file, then moves result to requested destination
        """
        tmp_fileobj = None
        try:
            # cleanup
            self.remove_existing_index_files()

            # write to temporary disk storage
            with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp_fileobj:
                logging.debug("Generating %s to temporary file %s", self.INDEX_NAME, tmp_fileobj.name)
                self.serialize_info(tmp_fileobj)

            # finalize name and location
            index_name = self.index_file_name()
            pkg_path = self._destination_directory.path / index_name
            logging.debug("Moving %s to %s", tmp_fileobj.name, pkg_path)
            shutil.move(tmp_fileobj.name, pkg_path)
            logging.info("Generated %s", pkg_path.name)

        finally:
            # cleanup temporary file
            if tmp_fileobj is not None:
                delete_file(tmp_fileobj.name)

    def generate(self):
        logging.info("Building index of packages")
        self.build_package_info()
        self.serialize()


class JsonIndexer(Indexer):

    def __init__(self, destination_directory):
        super().__init__(destination_directory)

    @classmethod
    def package_pattern(cls):
        return "{0}.jsonl".format(super().package_pattern())

    @classmethod
    def index_file_name(cls):
        """
        Implementation required in sub-classes
        """
        return "{0}.json".format(super().index_file_name())

    def serialize_index(self, file_obj):
        """
        Implementation required in sub-classes
        """
        json_data = json.dumps(self._package_info, sort_keys=True, indent=4)
        file_obj.write(json_data.encode())


class LzmaJsonIndexer(JsonIndexer):

    LZMA_SETTINGS = {
        "format": lzma.FORMAT_XZ,
        "check": lzma.CHECK_SHA256,
        "preset": lzma.PRESET_DEFAULT,
    }

    def __init__(self, destination_directory):
        super().__init__(destination_directory)

    @classmethod
    def package_pattern(cls):
        return "{0}.xz".format(super().package_pattern())

    @classmethod
    def index_file_name(cls):
        """
        Implementation required in sub-classes
        """
        return "{0}.xz".format(super().index_file_name())

    def serialize_info(self, file_obj):
        """
        Overrides default implementation
        Wraps serialization into a compressed file
        """
        with lzma.LZMAFile(file_obj, mode="w", **self.LZMA_SETTINGS) as lzma_fileobj:
            self.serialize_index(lzma_fileobj)
