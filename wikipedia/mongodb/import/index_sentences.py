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

    logger.info('indexing: id_ll')
    collection.create_index('source_id_ll', sparse=True)

    logger.info('indexing: title_ll')
    collection.create_index('source_title_ll', sparse=True)

    logger.info('indexing: len_links')
    collection.create_index('len_links')

    logger.info('indexing: len_links_ll')
    collection.create_index('len_links_ll')

    logger.info('indexing: links.id')
    collection.create_index('links.id')

    logger.info('indexing: links.title')
    collection.create_index('links.title')

    logger.info('indexing: links.text')
    collection.create_index('links.text')

    logger.info('indexing: tokens.text')
    collection.create_index('tokens.text')

    client.close()
    logger.info('done.')