import pickle
import functools

from pymongo import MongoClient
import numpy as np


class VectorStore(object):
    """Vector Store"""

    def __init__(self, host, port, db_name, collection_name):
        self.client = MongoClient(host=host, port=port)
        self.collection = self.client[db_name][collection_name]

    @functools.lru_cache(maxsize=None)
    def get_vector_by_item(self, item):
        response = self.collection.find_one({'item': item})
        if response:
            return pickle.loads(response['vector'])
        return None

    @functools.lru_cache(maxsize=None)
    def get_vector_by_index(self, idx):
        response = self.collection.find_one({'idx': idx})
        if response:
            return pickle.loads(response['vector'])
        return None
