import sys
import os
import logging
import argparse

from pymongo import MongoClient


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('host', help='MongoDB host')
    parser.add_argument('port', help='MongoDB port')
    parser.add_argument('db_name', help='Database name')
    parser.add_argument('collection_name', help='Collection name')
    args = parser.parse_args()

    host = args.host
    port = int(args.port)
    db_name = args.db_name
    collection_name = args.collection_name

    client = MongoClient(host=host, port=port)
    collection = client[db_name][collection_name]

    logger.info('indexing: _chunk_id')
    collection.create_index('_chunk_id')

    logger.info('indexing: id')
    collection.create_index('id')

    logger.info('indexing: title')
    collection.create_index('title')

    logger.info('indexing: redirect')
    collection.create_index('redirect')

    logger.info('indexing: disambiguation')
    collection.create_index('disambiguation')

    logger.info('indexing: categories')
    collection.create_index('categories')

    logger.info('indexing: sections.text')
    collection.create_index('sections.text')

    logger.info('indexing: links.id')
    collection.create_index('links.id')

    logger.info('indexing: links.title')
    collection.create_index('links.title')

    logger.info('indexing: links.text')
    collection.create_index('links.text')

    client.close()
    logger.info('done.')