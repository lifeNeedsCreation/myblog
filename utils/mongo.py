import os, sys
from pymongo import MongoClient

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR + "/utils")

from read_config import CONFIG

class Mongo(object):
    def __init__(self, section):
        self.client = MongoClient(CONFIG[section]["URI"])
        self.db = self.client[CONFIG[section]["DB"]]
        self.collection = self.db[CONFIG[section]["COLLECTION"]]

    def find_sync_tables(self, query):
        return self.collection.aggregate(query)

buzzbreak_mongo_client = Mongo("MONGO_ANALYTICS")
katkat_mongo_client = Mongo("MONGO_ANALYTICS")