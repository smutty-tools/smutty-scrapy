"""
foo
"""
import argparse
import configparser
import logging
import sys

import scrapy
import scrapy.crawler
import scrapy.utils.project
import sqlalchemy.engine.url

import smutty.scraper.settings

from smutty.scraper.spiders import SmuttySpider
from smutty.exceptions import SmuttyException
from smutty.filetools import IntegerStateFile


class App:
    """
    foo
    """

    def __init__(self):
        # analyze commande line arguments
        parser = argparse.ArgumentParser(description="Smutty metadata scrapper")
        parser.add_argument("-s", "--start-page", metavar="START_PAGE", type=int)
        parser.add_argument("-c", "--page-count", metavar="PAGE_COUNT", type=int)
        parser.add_argument("-m", "--min-id", metavar="MIN_ID", type=int)
        parser.add_argument("-b", "--blacklist-tag-file", metavar="BLACKLIST_FILE", type=str)
        parser.add_argument("config", metavar="CONFIG", type=str, nargs='?', default="smutty.conf")
        args = parser.parse_args()

        # exit early if nothing will be done
        if args.page_count is not None and int(args.page_count) == 0:
            sys.exit(0)

        # load configuration
        self._config = configparser.ConfigParser()
        try:
            with open(args.config) as config_fileobj:
                self._config.read_file(config_fileobj, source=args.config)
        except FileNotFoundError as exception:
            raise SmuttyException("Could not find file: {0}".format(exception))

        # process configuration
        try:
            self._current_page_state = IntegerStateFile(self._config['state_files']['current_page'])
            self._highest_id_state = IntegerStateFile(self._config['state_files']['highest_id'])
            self._lowest_id_state = IntegerStateFile(self._config['state_files']['lowest_id'])
            self._database_url = sqlalchemy.engine.url.URL(
                drivername=self._config['database']['dialect'],
                host=self._config['database']['host'],
                port=self._config['database']['port'],
                username=self._config['database']['username'],
                password=self._config['database']['password'],
                database=self._config['database']['database']
                )
        except KeyError as exception:
            raise SmuttyException("Problem while reading configuration: {0}".format(exception))

        # manage start page :
        # - start based on state file
        # - override if necessary
        # - persist to make sure it exists
        current_page = self._current_page_state.get()
        if args.start_page is not None:
            current_page = args.start_page
        if current_page is None or current_page == 0:
            current_page = 1
        self._current_page_state.set(current_page)

        # manage min id
        # - start based on state file
        # - override if necessary
        # - if defined, make sure state file exists
        min_id = self._lowest_id_state.get()
        if args.min_id is not None:
            min_id = args.min_id
        if min_id is not None:
            self._lowest_id_state.set(min_id)

        # manage settings
        self._settings = scrapy.utils.project.get_project_settings()
        self._settings.setmodule(smutty.scraper.settings)

        self._settings.set("SMUTTY_PAGE_COUNT", args.page_count)
        self._settings.set("SMUTTY_BLACKLIST_TAG_FILE", args.blacklist_tag_file)
        self._settings.set("SMUTTY_DATABASE_CONFIGURATION_URL", self._database_url)
        self._settings.set("SMUTTY_STATE_FILE_CURRENT_PAGE", self._config['state_files']['current_page'])
        self._settings.set("SMUTTY_STATE_FILE_HIGHEST_ID", self._config['state_files']['highest_id'])
        self._settings.set("SMUTTY_STATE_FILE_LOWEST_ID", self._config['state_files']['lowest_id'])

    def run(self):
        """
        foo
        """
        process = scrapy.crawler.CrawlerProcess(self._settings)
        process.crawl(SmuttySpider)
        process.start()  # it blocks here until finished


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
