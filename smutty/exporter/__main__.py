import argparse
import logging
import sys

import sqlalchemy

from smutty.db import DatabaseConfiguration, DatabaseSession
from smutty.config import ConfigurationFile
from smutty.exceptions import SmuttyException
from smutty.filetools import IntegerStateFile
from smutty.models import Item, create_all_tables

from smutty.exporter.serializers import LzmaJsonlPackageSerializer
from smutty.exporter.segments import Interval, Block
from smutty.exporter.packages import ImagePackage, VideoPackage


class App:

    def __init__(self):
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

        # analyze commande line arguments
        parser = argparse.ArgumentParser(description="Smutty metadata exporter")
        parser.add_argument("-o", "--output", help="output directory", default=False)
        parser.add_argument("-i", "--index-only", action='store_true', default=False)
        parser.add_argument("-m", dest="min_id", type=int)
        parser.add_argument("-M", dest="max_id", type=int)
        parser.add_argument("config", metavar="CONFIG", nargs='?', default=ConfigurationFile.DEFAULT_CONFIG_FILE)
        args = parser.parse_args()

        # load configuration
        self._config = ConfigurationFile(args.config)

        # setup state files
        self._highest_exporter_id_state = IntegerStateFile(self._config.get('exporter', 'highest_exporter_id'))
        self._lowest_exporter_id_state = IntegerStateFile(self._config.get('exporter', 'lowest_exporter_id'))
        self._lowest_scraper_id_state = IntegerStateFile(self._config.get('scraper', 'lowest_scraper_id'))

        # prepare target directory
        output_directory = args.output or self._config.get('exporter', 'output_directory')
        self._serializer = LzmaJsonlPackageSerializer(output_directory)

        # prepare database
        database_url = DatabaseConfiguration(self._config.get('database')).url
        self._database = DatabaseSession(database_url)
        create_all_tables(self._database.engine)

        # exporter limits
        self._exporter_max_id = args.max_id or self._highest_exporter_id_state.get()
        self._exporter_min_id = args.min_id or self._lowest_exporter_id_state.get()
        logging.info("Exporter current state limits : min_id=%s max_id=%s", self._exporter_min_id, self._exporter_max_id)
        if bool(self._exporter_min_id) ^ bool(self._exporter_max_id):
            raise SmuttyException("Either both exporter limits (or none !) should be specified")
        if self._exporter_min_id and self._exporter_max_id and self._exporter_min_id > self._exporter_max_id:
            raise SmuttyException("Exporter limits must respect min <= max")

        # scraper limits
        self._lowest_scraper_id = self._lowest_scraper_id_state.get()
        logging.info("Scraper current finished limit : min_id=%s", self._lowest_scraper_id)
        if self._lowest_scraper_id is None:
            logging.info("No scrap was finished, nothing to do: exiting")
            sys.exit(0)

        # database limits
        self._database_min_id, database_max_id = self.get_database_min_max_id()
        logging.info("Database current state limits : min_id=%s max_id=%s", self._database_min_id, database_max_id)
        if self._database_min_id is None or database_max_id is None:
            logging.info("Nothing in database, nothing to do: exiting")
            sys.exit(0)

        # cross validations
        if self._exporter_min_id and self._exporter_min_id < self._database_min_id:
            raise SmuttyException("Exporter low limit cannot be lower than database lower bound")
        if self._exporter_max_id and self._exporter_max_id > self._lowest_scraper_id:
            raise SmuttyException("Exporter high limit cannot be higher than scraper lower bound")

        # intervalss to process
        self._intervals = []

        # export everything and return if nothing was exported so far
        if self._exporter_min_id is None or self._exporter_max_id is None:
            logging.info("No exporter state available, exporting everything scraper produced")
            whole_range = Interval(self._database_min_id, self._lowest_scraper_id)
            self._intervals.append(whole_range)
            return

        # here the situation is expected to be
        assert self._database_min_id <= self._exporter_min_id <= self._exporter_max_id <= self._lowest_scraper_id

        # low boundary expansion (exported vs db)
        if self._database_min_id < self._exporter_min_id:
            lower_range = Interval(self._database_min_id, self._exporter_min_id)
            self._intervals.append(lower_range)
            logging.info("Queuing lower range expansion %s", lower_range)

        # high boundary expansion (exported vs scraped)
        if self._exporter_max_id < self._lowest_scraper_id:
            higher_range = Interval(self._exporter_max_id, self._lowest_scraper_id)
            self._intervals.append(higher_range)
            logging.info("Queuing higher range expansion %s", higher_range)

    def get_database_min_max_id(self):
        result = self._database.session.query(
            sqlalchemy.func.min(Item.item_id).label('min_id'),
            sqlalchemy.func.max(Item.item_id).label('max_id'),
        ).one()
        return (result.min_id, result.max_id)

    def run(self):
        # serialize items into packages
        for interval in self._intervals:
            logging.info("Exporting %s", interval)
            for block in Block.blocks_covering_interval(interval):
                self._serializer.serialize(ImagePackage(block), self._database.session)
                self._serializer.serialize(VideoPackage(block), self._database.session)
        # store progress in state files
        self._highest_exporter_id_state.set(self._lowest_scraper_id)
        self._lowest_exporter_id_state.set(self._database_min_id)


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
