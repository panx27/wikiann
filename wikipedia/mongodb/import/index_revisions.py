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
    parser.add_argument('--host', required=True, help='MongoDB host')
    parser.add_argument('--port', required=True, help='MongoDB port')
    parser.add_argument('--db_name', required=True, help='Database name')
    parser.add_argument('--collection_name', required=True, help='Collection name')
    parser.add_argument('--username', '-u', default=None,
                        help='Username (if authentication is enabled)')
    parser.add_argument('--password', '-p', default=None,
                        help='Password (if authentication is enabled)')
    args = parser.parse_args()

    host = args.host
    port = int(args.port)
    db_name = args.db_name
    collection_name = args.collection_name
    username = args.username
    password = args.password

    if username and password:
        client = MongoClient(host=host, port=port,
                             username=username, password=password)
    else:
        client = MongoClient(host=host, port=port)

    collection = client[db_name][collection_name]

    # logger.info('indexing: _chunk_id')
    # collection.create_index('_chunk_id')

    logger.info('indexing: ns')
    collection.create_index('ns')

    logger.info('indexing: page_id')
    collection.create_index('page_id')

    logger.info('indexing: page_title')
    collection.create_index('page_title')

    logger.info('indexing: redirect')
    collection.create_index('redirect', sparse=True)

    logger.info('indexing: id')
    collection.create_index('id')

    logger.info('indexing: parentid')
    collection.create_index('parentid', sparse=True)

    logger.info('indexing: ts')
    collection.create_index('ts')

    logger.info('indexing: idx')
    collection.create_index('idx')

    logger.info('indexing: comment')
    collection.create_index([('comment', 'text')])

    client.close()
    logger.info('done.')
