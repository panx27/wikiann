import sys
import logging
import argparse

import ujson as json
from pymongo import MongoClient


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def import_mentions(data, collection):
    to_insert = []
    logger.info('converting...') # TO-DO: save RAM
    for mention in data:
        if sys.getsizeof(mention) >= 1024:
            logger.warning('mention is too large, skip')
            continue

        entities = sorted(data[mention].items(),
                          key=lambda x: x[1], reverse=True)
        ins = {
            'mention': mention,
            'entities': entities
        }
        to_insert.append(ins)

    logger.info('importing...')
    try:
        collection.insert(to_insert)
    except Exception as e:
        logger.error('unexpected error')
        logger.exception(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('pdata', help='Path to input data')
    parser.add_argument('host', help='MongoDB host')
    parser.add_argument('port', help='MongoDB port')
    parser.add_argument('db_name', help='Database name')
    parser.add_argument('collection_name', help='Collection name')
    args = parser.parse_args()

    pdata = args.pdata
    host = args.host
    port = int(args.port)
    db_name = args.db_name
    collection_name = args.collection_name

    logger.info('loading data...')
    data = json.load(open(pdata))
    logger.info('done.')

    logger.info('db name: %s' % db_name)
    logger.info('collection name: %s' % collection_name)
    client = MongoClient(host=host, port=port)
    logger.info('drop collection')
    client[db_name].drop_collection(collection_name)

    collection = client[db_name][collection_name]
    logger.info('processing...')
    import_mentions(data, collection)
    logger.info('done.')

    logger.info('indexing...')
    collection.create_index('mention', unique=True)
    logger.info('done.')

    logger.info(collection)
    logger.info(collection.count())

    client.close()
