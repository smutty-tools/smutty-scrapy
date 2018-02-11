import json
import logging
import lzma
import shutil
import tempfile

from path import Path

from smutty.filetools import md5_file


class PackageSerializer:

    def __init__(self, destination_directory):
        self._destination_directory = Path(destination_directory).expand().abspath()
        self._destination_directory.mkdir_p()
        logging.info("Output directory is %s", self._destination_directory)

    def __repr__(self):
        return "{0}({_destination_directory})".format(self.__class__.__name__, **self.__dict__)

    @classmethod
    def serialize_package(cls, package, db_session, file_obj):
        for item in package.db_items(db_session):
            cls.serialize_item(item, file_obj)

    @classmethod
    def serialize_item(cls, item, file_obj):
        raise NotImplementedError()

    def serialize(self, package, db_session):
        raise NotImplementedError()


class JsonlPackageSerializer(PackageSerializer):

    def __init__(self, destination_directory):
        super().__init__(destination_directory)

    @classmethod
    def serialize_item(cls, item, file_obj):
        item = item.export_dict()
        # tags are sorted so that exporter output is stable
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

    def serialize(self, package, db_session):
        tmp_fileobj = None
        try:
            # write to temporary disk storage
            with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp_fileobj:
                logging.debug("Exporting %s to temporary file %s", package, tmp_fileobj.name)
                with lzma.LZMAFile(tmp_fileobj, mode="w", **self.LZMA_SETTINGS) as lzma_fileobj:
                    self.serialize_package(package, db_session, lzma_fileobj)

            # finalize name and location
            pkg_name = "{0}-{1}.jsonl.xz".format(package.name(), md5_file(tmp_fileobj.name))
            pkg_path = self._destination_directory / pkg_name
            logging.debug("Moving %s to %s", tmp_fileobj.name, pkg_path)
            shutil.move(tmp_fileobj.name, pkg_path)
            logging.info("Generated %s", pkg_path.name)

        finally:
            # cleanup temporary file
            if tmp_fileobj is not None:
                tmp_path = Path(tmp_fileobj.name)
                try:
                    tmp_path.remove()
                except FileNotFoundError:
                    pass
                else:
                    logging.debug("Removed temporary file %s", tmp_path)
