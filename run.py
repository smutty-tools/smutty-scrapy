#!/usr/bin/env python3

if __name__ == '__main__':

    import argparse
    import scrapy
    import scrapy.crawler
    import scrapy.utils.project
    import sqlalchemy.engine.url
    import sys

    # analyze commande line arguments
    parser = argparse.ArgumentParser(description="Smutty metadata scrapper")
    parser.add_argument("-s", "--start-page", metavar="START_PAGE", type=int, default=1)
    parser.add_argument("-c", "--page-count", metavar="PAGE_COUNT", type=int)
    parser.add_argument("-m", "--min-id", metavar="MIN_ID", type=int, nargs="?", const="-1")
    parser.add_argument("-b", "--blacklist-tag-file", metavar="BLACKLIST_FILE", type=str)
    parser.add_argument("-d", "--database", metavar="DATABASE_CONFIG", type=str, nargs="+")
    args = parser.parse_args()

    if args.page_count is not None and int(args.page_count) == 0:
        sys.exit(0)

    settings = scrapy.utils.project.get_project_settings()

    if args.database is not None:
        # drivername, host, port, username, password, database, query
        params = dict(s.split("=") for s in args.database)
        # drivername, host, port, username, password, database, query
        try:
            url = sqlalchemy.engine.url.URL(**params)
            print(url)
            settings.set("SMUTTY_DATABASE_CONFIGURATION_URL", url)
        except Exception as e:
            print("Error while parsing database configuration: {0} - {1}".format(type(e).__name__, e))
            sys.exit(1)

    if args.min_id is not None and args.min_id == -1:
        from smutty.models import Item, create_all_tables
        engine = sqlalchemy.create_engine(settings.get("SMUTTY_DATABASE_CONFIGURATION_URL"))
        create_all_tables(engine)
        Session = sqlalchemy.orm.sessionmaker(bind=engine)
        session = Session()
        latest = session.query(sqlalchemy.func.max(Item.item_id)).scalar()
        args.min_id = latest

    settings.set("SMUTTY_START_PAGE", args.start_page)
    settings.set("SMUTTY_PAGE_COUNT", args.page_count)
    settings.set("SMUTTY_MIN_ID", args.min_id)
    settings.set("SMUTTY_BLACKLIST_TAG_FILE", args.blacklist_tag_file)

    process = scrapy.crawler.CrawlerProcess(settings)
    process.crawl('smutty_spider')
    process.start()  # it blocks here until finished
