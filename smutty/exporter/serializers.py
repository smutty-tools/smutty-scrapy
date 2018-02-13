import json
import logging
import lzma
import shutil
import tempfile

from path import Path

from smutty.filetools import md5_file, delete_file


class PackageSerializer:

    def __init__(self, destination_directory):
        self._destination_directory = Path(destination_directory).expand().abspath()
        self._destination_directory.mkdir_p()
        logging.info("Output directory is %s", self._destination_directory)

    def __repr__(self):
        return "{0}({_destination_directory})".format(self.__class__.__name__, **self.__dict__)

    def remove_existing_package_files(self, package):
        pattern = "{0}-*".format(package.name())
        for file in self._destination_directory.files(pattern):
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
        tmp_fileobj = None
        try:
            # cleanup
            self.remove_existing_package_files(package)

            # write to temporary disk storage
            with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp_fileobj:
                logging.debug("Exporting %s to temporary file %s", package, tmp_fileobj.name)
                self.serialize_to_file(package, db_session, tmp_fileobj)

            # finalize name and location
            pkg_name = self.package_file_name(package, md5_file(tmp_fileobj.name))
            pkg_path = self._destination_directory / pkg_name
            logging.debug("Moving %s to %s", tmp_fileobj.name, pkg_path)
            shutil.move(tmp_fileobj.name, pkg_path)
            logging.info("Generated %s", pkg_path.name)

        finally:
            # cleanup temporary file
            if tmp_fileobj is not None:
                delete_file(tmp_fileobj.name)

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

    def __init__(self, destination_directory):
        super().__init__(destination_directory)

    @classmethod
    def package_file_name(cls, package, hash_digest):
        """
        IMPORTANT: Re-implementation required in sub-classes
        """
        return "{0}-{1}.jsonl".format(package.name(), hash_digest)

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

    LZMA_SETTINGS = {
        "format": lzma.FORMAT_XZ,
        "check": lzma.CHECK_SHA256,
        "preset": lzma.PRESET_DEFAULT,
    }

    def __init__(self, destination_directory):
        super().__init__(destination_directory)

    @classmethod
    def package_file_name(cls, package, hash_digest):
        return "{0}-{1}.jsonl.xz".format(package.name(), hash_digest)

    @classmethod
    def serialize_to_file(cls, package, db_session, file_obj):
        """
        Overrides default implementation
        Wraps serialization into a compressed file
        """
        with lzma.LZMAFile(file_obj, mode="w", **cls.LZMA_SETTINGS) as lzma_fileobj:
            cls.serialize_package(package, db_session, lzma_fileobj)
