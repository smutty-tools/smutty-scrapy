import contextlib
import datetime
import pytz
import scrapy
import time

from smutty.filetools import IntegerStateFile
from smutty.scraper.items import SmuttyImage, SmuttyVideo


class SmuttySpider(scrapy.Spider):

    name = "smutty_spider"
    allowed_domains = ["m.smutty.com"]

    _page_url = 'https://m.smutty.com/?view=new&home=1&page={0}&h=&lazy=1'
    _tag_blacklist = set()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get("SMUTTY_STATE_FILE_CURRENT_PAGE"),
                   crawler.settings.get("SMUTTY_STATE_FILE_HIGHEST_ID"),
                   crawler.settings.get("SMUTTY_STATE_FILE_LOWEST_ID"),
                   crawler.settings.get("SMUTTY_PAGE_COUNT"),
                   crawler.settings.get("SMUTTY_BLACKLIST_TAGS"))

    def __init__(self, current_page_state_file, highest_id_state_file, lowest_id_state_file, page_count, blacklist_tags):
        # init
        self._current_page_state = IntegerStateFile(current_page_state_file, self.logger)
        self._highest_id_state = IntegerStateFile(highest_id_state_file, self.logger)
        self._lowest_id_state = IntegerStateFile(lowest_id_state_file, self.logger)
        self._highest_id = self._highest_id_state.get()
        self._lowest_id = self._lowest_id_state.get()
        self.logger.info("State: highest_id={0} lowest_id={1} current_page={2}".format(self._highest_id, self._lowest_id, self._current_page_state.get()))
        self._tag_blacklist = blacklist_tags
        self.logger.info("Blacklisted tags: {0}".format(self._tag_blacklist))
        # limit
        self._end_page = None
        if page_count:
            self._end_page = self._current_page_state.get() + page_count

    def get_page_url(self, page_number):
        return self._page_url.format(page_number)

    def _request_page(self, page_number):
        # queue page for download
        meta = {"page_number": page_number}
        return scrapy.Request(url=self.get_page_url(page_number),
                              callback=self.parse,
                              meta=meta)

    def start_requests(self):
        yield self._request_page(self._current_page_state.get())

    def finalize_run(self):
        self.logger.info("Finalizing states")

        # trace before
        high = self._highest_id_state.get()
        low = self._lowest_id_state.get()
        cur_page = self._current_page_state.get()
        self.logger.info("Before: highest_id={0} lowest_id={1} current_page={2}".format(high, low, cur_page))

        # we memorize highest seen id as new lowest id, for next run
        self._lowest_id_state.set(self._highest_id)

        # we clean up for a new "from newest" run
        with contextlib.suppress(FileNotFoundError):
            self._highest_id_state.delete()    # we forget highest id
            self._current_page_state.delete()  # run will restart from first page

        # trace after
        high = self._highest_id_state.get()
        low = self._lowest_id_state.get()
        cur_page = self._current_page_state.get()
        self.logger.info("After: highest_id={0} lowest_id={1} current_page={2}".format(high, low, cur_page))

    def parse(self, response):
        self.logger.info("Parsing page {0}".format(response.meta["page_number"]))

        # save progression
        self._current_page_state.set(response.meta["page_number"])

        # find content
        divs = response.css("#container_chart").xpath("./div[@id]")

        # if nothing on page, consider we reached the end of the archive
        if not len(divs):
            self.finalize_run()
            return

        # handle content
        for block in divs:
            # id
            item_id = int(block.xpath(".//a/@onclick").re_first("^App\.txtr\((\d+)\)"))

            # tags
            tags = set(map(str.lower, block.xpath(
                ".//a/@href").re("^/h/(.*)/")))
            # skip unwanted items
            if tags is not None and tags & self._tag_blacklist:
                self.logger.info("Ignoring item id {0} due to blacklisted tag".format(item_id))
                continue

            # set highest id if not already set
            if self._highest_id is None:
                self.logger.info("Memorizing {0} as highest id".format(item_id))
                self._highest_id = item_id
                self._highest_id_state.set(item_id)

            # check for minimum bound
            if self._lowest_id and item_id <= self._lowest_id:
                self.logger.info("Reached id {0} which is below lowest id {1} as highest id".format(item_id, self._lowest_id))
                self.finalize_run()
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
                yield SmuttyVideo(
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
                yield SmuttyImage(
                    # SmuttyItem
                    item_id=item_id,
                    submitter=submitter,
                    sub_page=sub_page,
                    tags=tags,
                    last_updated=last_updated,
                    # SmuttyImage
                    image_url=image
                )

        # go on if necessary
        if self._end_page is None or response.meta["page_number"] < self._end_page - 1:
            yield self._request_page(response.meta["page_number"] + 1)
