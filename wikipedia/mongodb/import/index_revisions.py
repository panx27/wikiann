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

    logger.info('indexing: ns')
    collection.create_index('ns')

    logger.info('indexing: pid')
    collection.create_index('pid')

    logger.info('indexing: title')
    collection.create_index('title')

    logger.info('indexing: revid')
    collection.create_index('revid')

    logger.info('indexing: ts')
    collection.create_index('ts')

    logger.info('indexing: idx')
    collection.create_index('idx')

    logger.info('indexing: comment')
    key = [('comment', 1)]
    pfe = {'comment': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    logger.info('indexing: parent_revid')
    key = [('parent_revid', 1)]
    pfe = {'parent_revid': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    logger.info('indexing: last')
    key = [('last', 1)]
    pfe = {'last': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    logger.info('indexing: title: 1, last: 1')
    key = [('title', 1), ('last', 1)]
    pfe = {'last': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    logger.info('indexing: ts: 1, last: 1')
    key = [('ts', 1), ('last', 1)]
    pfe = {'last': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    logger.info('indexing: ts: 1, first: 1')
    key = [('ts', 1), ('first', 1)]
    pfe = {'first': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    logger.info('indexing: contributor.name')
    key = [('contributor.name', 1)]
    pfe = {'contributor.name': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    client.close()
    logger.info('done.')
