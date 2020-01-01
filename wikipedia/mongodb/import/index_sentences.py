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

    logger.info('indexing: source_id')
    collection.create_index('source_id')

    logger.info('indexing: source_title')
    collection.create_index('source_title')

    logger.info('indexing: source_id_ll')
    collection.create_index('source_id_ll', sparse=True)

    logger.info('indexing: source_title_ll')
    collection.create_index('source_title_ll', sparse=True)

    logger.info('indexing: start')
    collection.create_index('start')

    logger.info('indexing: end')
    collection.create_index('end')

    logger.info('indexing: ids_len')
    collection.create_index('ids_len')

    logger.info('indexing: ids_ll_len')
    collection.create_index('ids_ll_len')

    logger.info('indexing: ids')
    key = [('ids', 1)]
    pfe = {'ids': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    logger.info('indexing: ids_ll')
    key = [('ids_ll', 1)]
    pfe = {'ids_ll': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    logger.info('indexing: titles')
    key = [('titles', 1)]
    pfe = {'titles': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    logger.info('indexing: titles_ll')
    key = [('titles_ll', 1)]
    pfe = {'titles_ll': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    logger.info('indexing: source_id & ids')
    key = [('source_id', 1), ('ids', 1)]
    pfe = {'ids': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    logger.info('indexing: keywords')
    collection.create_index('keywords')

    client.close()
    logger.info('done.')
