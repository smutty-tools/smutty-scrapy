# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class SmuttyItem(scrapy.Item):
    item_id = scrapy.Field()
    submitter = scrapy.Field()
    sub_page = scrapy.Field()
    tags = scrapy.Field()
    last_updated = scrapy.Field(serializer=str)


class SmuttyImage(SmuttyItem):
    image_url = scrapy.Field()


class SmuttyVideo(SmuttyItem):
    poster_url = scrapy.Field()
    video_url = scrapy.Field()
    video_mime = scrapy.Field()
