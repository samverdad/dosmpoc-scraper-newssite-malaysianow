# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from azure.storage.blob import BlobServiceClient
import json
import os
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

def get_blob_service_client():
    connection_string = os.environ["AZ_SA_CONNECTION_STRING"]
    return BlobServiceClient.from_connection_string(connection_string)

class DosmpocScraperMalaysianowPipeline:
    def __init__(self):
        self.blob_service_client = get_blob_service_client()
        self.items = []

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def open_spider(self, spider):
        self.container_client = self.blob_service_client.get_container_client(os.environ["AZ_SA_BLOB_CONTAINER_NAME"])

    def close_spider(self, spider):
        items_json = json.dumps(self.items, ensure_ascii=False)
        
        filename = f"output_malaysianow_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"

        blob_client = self.container_client.get_blob_client(filename)
        blob_client.upload_blob(items_json, overwrite=True)

    def process_item(self, item, spider):
        self.items.append(dict(item))
        return item
