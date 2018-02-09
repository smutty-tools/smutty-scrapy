import argparse
import logging
import sys

import sqlalchemy

from smutty.db import DatabaseConfiguration, DatabaseSession
from smutty.config import ConfigurationFile
from smutty.exceptions import SmuttyException
from smutty.filetools import IntegerStateFile
from smutty.models import Item, create_all_tables


class App:
    """
    foo
    """

    def __init__(self):
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
        self.logger = logging.getLogger('exporter')

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
        self._highest_scraper_id_state = IntegerStateFile(self._config.get('scraper', 'highest_scraper_id'))
        self._lowest_scraper_id_state = IntegerStateFile(self._config.get('scraper', 'lowest_scraper_id'))
        database_url = DatabaseConfiguration(self._config.get('database')).url

        # prepare database
        self._database = DatabaseSession(database_url)
        create_all_tables(self._database.engine)

        # state limits
        self._exporter_max_id = args.max_id or self._highest_exporter_id_state.get()
        self._exporter_min_id = args.min_id or self._lowest_exporter_id_state.get()
        self.logger.info("Exporter current state limits : min_id={0} max_id={1}".format(self._exporter_min_id, self._exporter_max_id))
        if bool(self._exporter_min_id) ^ bool(self._exporter_max_id):
            raise SmuttyException("Either both exporter limits (or none) should be specified")

        # database limits
        database_min_id, database_max_id = self.get_database_min_max_id()
        self.logger.info("Database current state limits : min_id={0} max_id={1}".format(database_min_id, database_max_id))
        if database_min_id is None or database_max_id is None or database_min_id == database_max_id:
            self.logger.info("Nothing in database, nothing to do: exiting")
            sys.exit(0)

        # define ranges
        self._segments = []
        if self._exporter_min_id is None or self._exporter_max_id is None:
            whole_range = (database_min_id, database_max_id)
            self._segments.append(whole_range)
        else:
            if database_min_id < self._exporter_min_id:
                lower_range = (database_min_id, self._exporter_min_id)
                self._segments.append(lower_range)
            if self._exporter_max_id < database_max_id:
                higher_range = (self._exporter_max_id, database_max_id)
                self._segments.append(higher_range)

    def get_database_min_max_id(self):
        result = self._database.session.query(
            sqlalchemy.func.min(Item.item_id).label('min_id'),
            sqlalchemy.func.max(Item.item_id).label('max_id'),
        ).one()
        return (result.min_id, result.max_id)

    def run(self):
        """
        foo
        """
        for segment in self._segments:
            print(segment)


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
