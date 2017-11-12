# -*- coding: utf-8 -*-

import datetime
import pytz
import scrapy
import smutty.items
import time


class SmuttySpiderSpider(scrapy.Spider):
    name = "smutty_spider"
    allowed_domains = ["m.smutty.com"]

    page_url = 'https://m.smutty.com/?view=new&home=1&page={0}&h=&lazy=1'

    tag_blacklist = set()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get("SMUTTY_START_PAGE"),
                   crawler.settings.get("SMUTTY_PAGE_COUNT"),
                   crawler.settings.get("SMUTTY_MIN_ID"),
                   crawler.settings.get("SMUTTY_BLACKLIST_TAG_FILE"))

    @classmethod
    def load_blacklist_tag(cls, blacklist_tag_file):
        if not blacklist_tag_file:
            return
        with open(blacklist_tag_file) as file_obj:
            cls.tag_blacklist = set(map(str.lower, file_obj.read().split()))

    def __init__(self, start_page, page_count, min_id, blacklist_tag_file):
        self.start_page = start_page
        self.end_page = None
        if page_count is not None:
            self.end_page = self.start_page + page_count
        self.min_id = min_id
        self.load_blacklist_tag(blacklist_tag_file)

    def get_page_url(self, page_number):
        return self.page_url.format(page_number)

    def request_page(self, page_number):
        meta = {"page_number": page_number}
        return scrapy.Request(url=self.get_page_url(page_number),
                              callback=self.parse,
                              meta=meta)

    def start_requests(self):
        yield self.request_page(self.start_page)

    def parse(self, response):
        meta = response.meta

        # find content
        divs = response.css("#container_chart").xpath("./div[@id]")

        # check for content
        if len(divs) == 0:
            return

        # handle content
        for block in divs:
            # tags
            tags = set(map(str.lower, block.xpath(
                ".//a/@href").re("^/h/(.*)/")))
            # skip unwanted items
            if tags is not None and tags & self.tag_blacklist:
                continue
            # id
            item_id = int(block.xpath(".//a/@onclick").re_first("^App\.txtr\((\d+)\)"))
            if self.min_id is not None and item_id <= self.min_id:
                return
            # subitter
            submitter = block.xpath('.//img[@onclick]/@alt').extract_first()
            # content
            content = block.css("div.center a")
            sub_page = content.xpath("./@href").extract_first()
            # image
            image = content.xpath(".//img/@src").extract_first()
            # timestamp
            last_updated = datetime.datetime.fromtimestamp(time.time(), pytz.UTC)
            # finalize item
            if image is None:
                video = content.xpath(".//video")
                yield smutty.items.SmuttyVideo(
                    # SmuttyItem
                    item_id=item_id,
                    submitter=submitter,
                    sub_page=sub_page,
                    tags=tags,
                    last_updated=last_updated,
                    # SmuttyVideo
                    poster_url=video.xpath("./@poster").extract_first(),
                    video_url=video.xpath("./source/@src").extract_first(),
                    video_mime=video.xpath("./source/@type").extract_first()
                )
            else:
                yield smutty.items.SmuttyImage(
                    # SmuttyItem
                    item_id=item_id,
                    submitter=submitter,
                    sub_page=sub_page,
                    tags=tags,
                    last_updated=last_updated,
                    # SmuttyImage
                    image_url=image
                )

        # queue next page
        if self.end_page is None or self.end_page is not None and meta["page_number"] < self.end_page - 1:
            yield self.request_page(meta["page_number"] + 1)
