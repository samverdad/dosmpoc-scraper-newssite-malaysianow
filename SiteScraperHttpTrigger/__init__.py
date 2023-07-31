import logging
import os
import sys

import azure.functions as func
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dosmpoc_scraper_malaysianow")))

from dosmpoc_scraper_malaysianow.spiders.site_scraper_spider import SiteScraperSpider


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'dosmpoc_scraper_malaysianow.settings')

    runner = CrawlerRunner(get_project_settings())
    runner.crawl(SiteScraperSpider)

    reactor.run()

    return func.HttpResponse("Done", status_code=200)
