from sqlalchemy import Column, Integer, String, DateTime, ForeignKey

import sqlalchemy.ext.declarative

DeclarativeBase = sqlalchemy.ext.declarative.declarative_base(
    metadata=sqlalchemy.MetaData(schema='smutty')
)

association_item_tag = sqlalchemy.Table(
    'association_item_tag',
    DeclarativeBase.metadata,
    Column('item_id', Integer,
           ForeignKey('items.item_id', ondelete='CASCADE'),
           primary_key=True),
    Column('tag_id', Integer,
           ForeignKey('tags.tag_id', ondelete='CASCADE'),
           primary_key=True)
)


class Tag(DeclarativeBase):
    __tablename__ = 'tags'

    tag_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

    items = sqlalchemy.orm.relationship(
        'Item',
        collection_class=set,
        passive_deletes=True,
        secondary=association_item_tag,
        back_populates='tags'
    )


class Item(DeclarativeBase):
    __tablename__ = 'items'

    # common attributes
    item_id = Column(Integer, primary_key=True)
    submitter = Column(String, nullable=False, index=True)
    sub_page = Column(String, nullable=False)
    last_updated = Column(DateTime(timezone=True), nullable=False)

    tags = sqlalchemy.orm.relationship(
        'Tag',
        collection_class=set,
        passive_deletes=True,
        secondary=association_item_tag,
        back_populates='items'
    )

    # for inheritance
    item_type = Column(Integer, nullable=False)

    __mapper_args__ = {
        'polymorphic_on': item_type,
        'polymorphic_identity': 0,
    }

    def export_dict(self, with_tags=True):
        d = {
            "item_id": self.item_id,
            "submitter": self.submitter,
            "sub_page": self.sub_page,
            "last_updated": self.last_updated.__str__()
        }
        if with_tags:
            d["tags"] = [t.name for t in self.tags]
        return d


class Image(Item):
    __tablename__ = 'images'

    # unique attributes
    image_url = Column(String, nullable=False, unique=True)

    # link to parent
    item_id = Column(Integer,
                     ForeignKey('items.item_id', ondelete='CASCADE'),
                     primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity': 1,
    }

    def export_dict(self, with_tags=True):
        d = super().export_dict(with_tags)
        d["image_url"] = self.image_url
        return d


class Video(Item):
    __tablename__ = 'videos'

    # unique attributes
    poster_url = Column(String, nullable=False, unique=True)
    video_url = Column(String, nullable=False, unique=True)
    video_mime = Column(String, nullable=False)

    # link to parent
    item_id = Column(Integer,
                     ForeignKey('items.item_id', ondelete='CASCADE'),
                     primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity': 2,
    }

    def export_dict(self, with_tags=True):
        d = super().export_dict(with_tags)
        d["poster_url"] = self.poster_url
        d["video_url"] = self.video_url
        d["video_mime"] = self.video_mime
        return d


# must be last as it collects info from previous table declarations
def create_all_tables(engine):
    DeclarativeBase.metadata.create_all(engine)
