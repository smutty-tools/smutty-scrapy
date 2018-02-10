import argparse
import json
import logging
import lzma
import shutil
import sys
import tempfile

import sqlalchemy

from path import Path

from smutty.db import DatabaseConfiguration, DatabaseSession
from smutty.config import ConfigurationFile
from smutty.exceptions import SmuttyException
from smutty.filetools import IntegerStateFile, md5_file
from smutty.models import Item, Image, Video, create_all_tables


class Interval:

    def __init__(self, min_id, max_id):
        assert min_id <= max_id
        self.min_id = min_id
        self.max_id = max_id

    def __repr__(self):
        return "{0}({min_id}, {max_id})".format(self.__class__.__name__, **self.__dict__)


class Block(Interval):

    SIZE = 10000

    def __init__(self, min_id, max_id):
        super().__init__(min_id, max_id)

    @classmethod
    def blocks_covering_interval(cls, interval):
        assert cls.SIZE > 0
        lowest_id = interval.min_id - interval.min_id % cls.SIZE
        highest_id = interval.max_id - interval.max_id % cls.SIZE
        for base in range(lowest_id, highest_id + 1, cls.SIZE):
            yield Block(base, base + cls.SIZE - 1)

    def items(self, db_session, item_class):
        # query is sorted so that exporter output is stable
        return db_session.query(item_class).filter(
                self.min_id <= item_class.item_id,
                item_class.item_id <= self.max_id
            ).order_by(item_class.item_id)


class Package:

    def __init__(self, block, item_class):
        self._block = block
        self._item_class = item_class

    def __repr__(self):
        return "{0}({_block}, {_item_class.__name__})".format(self.__class__.__name__, **self.__dict__)

    def db_items(self, db_session):
        return self._block.items(db_session, self._item_class)

    def name(self):
        return "{0}-{1}-{2}".format(
            self._item_class.__name__.lower(),
            self._block.min_id,
            self._block.max_id)


class ImagePackage(Package):

    def __init__(self, block):
        super().__init__(block, Image)


class VideoPackage(Package):

    def __init__(self, block):
        super().__init__(block, Video)


class Exporter:

    LZMA_SETTINGS = {
        "format": lzma.FORMAT_XZ,
        "check": lzma.CHECK_SHA256,
        "preset": lzma.PRESET_DEFAULT,
    }

    def __init__(self, destination_directory):
        self._destination_directory = Path(destination_directory).expand().abspath()
        self._destination_directory.mkdir_p()
        logging.info("Output directory is %s", self._destination_directory)

    def __repr__(self):
        return "{0}({_destination_directory}, {with_tags})".format(self.__class__.__name__, **self.__dict__)

    @staticmethod
    def export_to_jsonl(db_session, package, file_obj):
        for item in package.db_items(db_session):
            item = item.export_dict()
            # tags are sorted so that exporter output is stable
            item['tags'].sort()
            json_data = json.dumps(item, sort_keys=True)
            file_obj.write(json_data.encode())
            file_obj.write("\n".encode())

    def export(self, db_session, package):
        tmp_fileobj = None
        try:
            # write to temporary disk storage
            with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp_fileobj:
                logging.debug("Exporting %s to temporary file %s", package, tmp_fileobj.name)
                with lzma.LZMAFile(tmp_fileobj, mode="w", **self.LZMA_SETTINGS) as lzma_fileobj:
                    self.export_to_jsonl(db_session, package, lzma_fileobj)

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


class App:

    def __init__(self):
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

        # analyze commande line arguments
        parser = argparse.ArgumentParser(description="Smutty metadata exporter")
        parser.add_argument("-i", "--index-only", action='store_true', default=False)
        parser.add_argument("-m", dest="min_id", type=int)
        parser.add_argument("-M", dest="max_id", type=int)
        parser.add_argument("config", metavar="CONFIG", nargs='?', default=ConfigurationFile.DEFAULT_CONFIG_FILE)
        args = parser.parse_args()

        # load configuration
        self._config = ConfigurationFile(args.config)
        self._highest_exporter_id_state = IntegerStateFile(self._config.get('exporter', 'highest_exporter_id'))
        self._lowest_exporter_id_state = IntegerStateFile(self._config.get('exporter', 'lowest_exporter_id'))
        self._lowest_scraper_id_state = IntegerStateFile(self._config.get('scraper', 'lowest_scraper_id'))
        self._exporter = Exporter(self._config.get('exporter', 'output_directory'))
        database_url = DatabaseConfiguration(self._config.get('database')).url

        # prepare database
        self._database = DatabaseSession(database_url)
        create_all_tables(self._database.engine)

        # exporter limits
        self._exporter_max_id = args.max_id or self._highest_exporter_id_state.get()
        self._exporter_min_id = args.min_id or self._lowest_exporter_id_state.get()
        logging.info("Exporter current state limits : min_id={0} max_id={1}".format(self._exporter_min_id, self._exporter_max_id))
        if bool(self._exporter_min_id) ^ bool(self._exporter_max_id):
            raise SmuttyException("Either both exporter limits (or none !) should be specified")
        if self._exporter_min_id and self._exporter_max_id and self._exporter_min_id > self._exporter_max_id:
            raise SmuttyException("Exporter limits must respect min <= max")

        # scraper limits
        self._lowest_scraper_id = self._lowest_scraper_id_state.get()
        logging.info("Scraper current finished limit : min_id={0}".format(self._lowest_scraper_id))
        if self._lowest_scraper_id is None:
            logging.info("Nothing scrap was finished, nothing to do: exiting")
            sys.exit(0)

        # database limits
        database_min_id, database_max_id = self.get_database_min_max_id()
        logging.info("Database current state limits : min_id={0} max_id={1}".format(database_min_id, database_max_id))
        if database_min_id is None or database_max_id is None or database_min_id == database_max_id:
            logging.info("Nothing in database, nothing to do: exiting")
            sys.exit(0)

        # cross validations
        if self._exporter_min_id and self._exporter_min_id < database_min_id:
            raise SmuttyException("Exporter low limit cannot be lower than database lower bound")
        if self._exporter_max_id and self._exporter_max_id > self._lowest_scraper_id:
            raise SmuttyException("Exporter high limit cannot be higher than scraper lower bound")

        # intervalss to process
        self._intervals = []

        # export everything and return if nothing was exported so far
        if self._exporter_min_id is None or self._exporter_max_id is None:
            logging.info("No exporter state available, exporting everything scraper produced")
            whole_range = Interval(database_min_id, database_max_id)
            self._intervals.append(whole_range)
            return

        # here the situation is expected to be
        assert database_min_id <= self._exporter_min_id <= self._exporter_max_id <= self._lowest_scraper_id

        # low boundary expansion (exported vs db)
        if database_min_id < self._exporter_min_id:
            lower_range = Interval(database_min_id, self._exporter_min_id)
            self._intervals.append(lower_range)
            logging.info("Queuing lower range expansion {0}".format(lower_range))

        # high boundary expansion (exported vs scraped)
        if self._exporter_max_id < self._lowest_scraper_id:
            higher_range = Interval(self._exporter_max_id, self._lowest_scraper_id)
            self._intervals.append(higher_range)
            logging.info("Queuing higher range expansion {0}".format(higher_range))

    def get_database_min_max_id(self):
        result = self._database.session.query(
            sqlalchemy.func.min(Item.item_id).label('min_id'),
            sqlalchemy.func.max(Item.item_id).label('max_id'),
        ).one()
        return (result.min_id, result.max_id)

    def run(self):
        for interval in self._intervals:
            logging.info("Exporting {0}".format(interval))
            for block in Block.blocks_covering_interval(interval):
                self._exporter.export(self._database.session, ImagePackage(block))
                self._exporter.export(self._database.session, VideoPackage(block))


def main():
    """
    foo
    """
    try:
        app = App()
        app.run()
    except SmuttyException as exception:
        logging.error("%s", exception)
    except Exception as exception:
        logging.critical("%s: %s", exception.__class__.__name__, exception)
        raise


# run
main()
