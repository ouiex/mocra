"""scrapy-redis benchmark spider (direct-item crawl).

Item URLs are pushed to Redis (`bench:start_urls`); N workers share the Redis scheduler and each
parses an item page (CSS selectors + text) and bumps a Redis counter the driver polls.
"""
from scrapy_redis.spiders import RedisSpider


class ItemSpider(RedisSpider):
    name = "itembench"
    redis_key = "bench:start_urls"

    def parse(self, response):
        # Real parse work, like a normal scraper.
        _title = response.css("h1.title::text").get()
        _price = response.css("span.price::text").get()
        _desc = response.css("p.desc::text").get()
        self.server.incr("bench:count")
