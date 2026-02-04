import os
import time
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Tuple

from benchmark_prod_like import run_benchmark as run_mocra_benchmark


def start_local_server() -> Tuple[HTTPServer, str]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, format, *args):
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address
    url = f"http://{host}:{port}/test"

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, url


def run_scrapy_redis_benchmark(count: int, base_url: str, redis_url: str) -> float:
    try:
        import scrapy  # noqa: F401
        from scrapy.crawler import CrawlerProcess
        from scrapy import signals
        from scrapy_redis.spiders import RedisSpider
        import redis
    except Exception as exc:
        print(f"Scrapy or scrapy-redis not installed: {exc}")
        print("Install with: uv add scrapy scrapy-redis")
        return 0.0

    key = "scrapy:benchmark:start_urls"
    r = redis.Redis.from_url(redis_url)
    r.delete(key)
    for i in range(count):
        r.rpush(key, f"{base_url}?_i={i}")

    processed = {"count": 0}
    start = time.time()
    scrapy_concurrency = int(os.environ.get("SCRAPY_CONCURRENCY", "256"))

    class BenchmarkSpider(RedisSpider):
        name = "benchmark"
        redis_key = key
        custom_settings = {
            "SCHEDULER": "scrapy_redis.scheduler.Scheduler",
            "DUPEFILTER_CLASS": "scrapy_redis.dupefilter.RFPDupeFilter",
            "SCHEDULER_PERSIST": False,
            "REDIS_URL": redis_url,
            "CONCURRENT_REQUESTS": scrapy_concurrency,
            "CONCURRENT_REQUESTS_PER_DOMAIN": scrapy_concurrency,
            "DOWNLOAD_DELAY": 0,
            "DOWNLOAD_TIMEOUT": 5,
            "RETRY_ENABLED": False,
            "REDIRECT_ENABLED": False,
            "COOKIES_ENABLED": False,
            "TELNETCONSOLE_ENABLED": False,
            "AUTOTHROTTLE_ENABLED": False,
            "DNSCACHE_ENABLED": True,
            "REACTOR_THREADPOOL_MAXSIZE": 32,
            "SCHEDULER_IDLE_BEFORE_CLOSE": 0.1,
            "LOG_ENABLED": False,
            "LOG_LEVEL": "INFO",
            "LOG_STDOUT": False,
        }

        def parse(self, response):
            yield {"url": response.url}

    process = CrawlerProcess()
    crawler = process.create_crawler(BenchmarkSpider)

    def on_item_scraped(item, response, spider):
        processed["count"] += 1
        if processed["count"] >= count:
            crawler.engine.close_spider(spider, "done")

    crawler.signals.connect(on_item_scraped, signal=signals.item_scraped)
    process.crawl(crawler)
    process.start()

    duration = time.time() - start
    if duration <= 0:
        return 0.0
    return processed["count"] / duration


def main():
    count = int(os.environ.get("BENCH_COUNT", "2000"))
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

    server, url = start_local_server()
    os.environ["MOCK_DEV_URL"] = url
    os.environ["MOCK_DEV_COUNT"] = str(count)

    print("Running Mocra prod-like benchmark...")
    mocra_start = time.time()
    throughput_mocra = 0.0
    try:
        import asyncio
        asyncio.run(run_mocra_benchmark(count))
        mocra_duration = time.time() - mocra_start
        throughput_mocra = count / mocra_duration if mocra_duration > 0 else 0.0
    except Exception as exc:
        print(f"Mocra benchmark failed: {exc}")

    print("Running Scrapy-Redis benchmark...")
    throughput_scrapy = run_scrapy_redis_benchmark(count, url, redis_url)

    print("\n--- Compare Results ---")
    print(f"Mocra Throughput: {throughput_mocra:.2f} items/s")
    if throughput_scrapy > 0:
        print(f"Scrapy-Redis Throughput: {throughput_scrapy:.2f} items/s")
    else:
        print("Scrapy-Redis Throughput: N/A (missing deps)")

    server.shutdown()


if __name__ == "__main__":
    main()
