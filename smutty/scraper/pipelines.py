import logging

from smutty.db import DatabaseSession
from smutty.models import Tag, Item, Image, Video, create_all_tables
from smutty.scraper.items import SmuttyImage, SmuttyVideo


class SmuttyDatabasePipeline:

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get("SMUTTY_DATABASE_CONFIGURATION_URL"))

    def __init__(self, database_configuration_url):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("Using database url: %s", database_configuration_url)
        self._database = DatabaseSession(database_configuration_url)
        # initialize tables if they do not exist
        create_all_tables(self._database.engine)

    def get_tags(self, session, tags):
        # fetch existing tags
        existing = {
            name: session.query(Tag).filter_by(name=name).first()
            for name in tags
        }
        # instanciate new ones
        existing = set(
            Tag(name=k) if v is None else v
            for k, v in existing.items()
        )
        return existing

    def wrap_tags(self, session, item):
        # replace text tags with ORM Tag
        orm_item = {
            k: v if k != 'tags'
            else self.get_tags(session, item['tags'])
            for k, v in item.items()
        }
        return orm_item

    def process_image(self, session, item):
        tagged_item = self.wrap_tags(session, item)
        image = Image(**tagged_item)
        return image

    def process_video(self, session, item):
        tagged_item = self.wrap_tags(session, item)
        video = Video(**tagged_item)
        return video

    def save_item(self, session, orm_item):
        try:
            session.add(orm_item)
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def process_item(self, item, spider):
        # create session
        session = self._database.session

        # item already exists
        item_id = item["item_id"]
        if session.query(Item).filter_by(item_id=item_id).first():
            self.logger.debug("Item %d already exists, skipping", item_id)
            return item

        # persist items
        if isinstance(item, SmuttyImage):
            orm_item = self.process_image(session, item)
            self.logger.debug("Saving image id %d", item_id)
            self.save_item(session, orm_item)
        elif isinstance(item, SmuttyVideo):
            orm_item = self.process_video(session, item)
            self.logger.debug("Saving video id %d", item_id)
            self.save_item(session, orm_item)
        else:
            self.logger.warning("Not processing unknown element: %s", item)

        # feed to other pipelines
        return item
