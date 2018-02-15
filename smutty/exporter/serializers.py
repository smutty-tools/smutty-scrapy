import json
import logging

from smutty.compression import LzmaCompression
from smutty.filetools import md5_file, FinalizedTempFile


class PackageSerializer:

    def __init__(self, destination_directory, file_mode):
        self._destination_directory = destination_directory
        self._file_mode = file_mode

    def __repr__(self):
        return "{0}({_destination_directory})".format(self.__class__.__name__, **self.__dict__)

    def remove_existing_package_files(self, package):
        pattern = "{0}-*".format(package.name())
        for file in self._destination_directory.path.files(pattern):
            logging.debug("Deleting present package file %s", file)
            file.remove()

    @classmethod
    def serialize_package(cls, package, db_session, file_obj):
        """
        Order items by id so that exporter output is stable
        """
        for item in package.db_items(db_session, sorted_by_id=True):
            cls.serialize_item(item, file_obj)

    def serialize(self, package, db_session):
        """
        Serialize to a temporary file, then moves result to requested destination
        """
        self.remove_existing_package_files(package)
        # first pass: generate content
        pkg_name = self.package_file_name(package, "INTERMEDIATE")
        pkg_path_intermediate = self._destination_directory.path / pkg_name
        with FinalizedTempFile(pkg_path_intermediate, self._file_mode) as tmp_fileobj:
            logging.debug("Exporting %s to temporary file %s", package, tmp_fileobj.name)
            self.serialize_to_file(package, db_session, tmp_fileobj)
        # second pass: generate hash and rename
        hash_digest = md5_file(pkg_path_intermediate)
        pkg_name = self.package_file_name(package, hash_digest)
        pkg_path_final = self._destination_directory.path / pkg_name
        pkg_path_intermediate.rename(pkg_path_final)
        logging.info("Generated package file %s", pkg_path_final)

    @classmethod
    def serialize_to_file(cls, package, db_session, file_obj):
        """
        Provide a default pass-through implementation
        """
        cls.serialize_package(package, db_session, file_obj)

    @classmethod
    def package_file_name(cls, package):
        """
        Implementation required in sub-classes
        """
        raise NotImplementedError()

    @classmethod
    def serialize_item(cls, item, file_obj):
        """
        Implementation required in sub-classes
        """
        raise NotImplementedError()


class JsonlPackageSerializer(PackageSerializer):

    def __init__(self, destination_directory, file_mode):
        super().__init__(destination_directory, file_mode)

    @classmethod
    def package_file_name(cls, package, suffix):
        """
        IMPORTANT: Re-implementation required in sub-classes
        """
        return "{0}-{1}.jsonl".format(package.name(), suffix)

    @classmethod
    def serialize_item(cls, item, file_obj):
        """
        Provide a default implementation for sub-classes
        Tags are sorted so that exporter output is stable
        """
        item = item.export_dict()
        item['tags'].sort()
        json_data = json.dumps(item, sort_keys=True)
        file_obj.write(json_data.encode())
        file_obj.write("\n".encode())


class LzmaJsonlPackageSerializer(JsonlPackageSerializer):

    def __init__(self, destination_directory, file_mode):
        super().__init__(destination_directory, file_mode)

    @classmethod
    def package_file_name(cls, package, suffix):
        return "{0}-{1}.jsonl.xz".format(package.name(), suffix)

    def serialize_to_file(self, package, db_session, file_obj):
        """
        Overrides default implementation
        Wraps serialization into a compressed file
        """
        with LzmaCompression(file_obj, self._file_mode) as lzma_fileobj:
            self.serialize_package(package, db_session, lzma_fileobj)
