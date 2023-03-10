import sys
import logging
import argparse
import io
import pickle

from bson.binary import Binary
from pymongo import MongoClient
import ujson as json
import numpy as np


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def import_vectors(pdata, collection):
    to_insert = []
    word2id = {}
    logger.info('loading...')
    with io.open(pdata, 'r', encoding='utf-8', newline='\n',
                 errors='ignore') as f:
        next(f)
        for i, line in enumerate(f):
            word, vect = line.rstrip().split(' ', 1)
            vect = np.fromstring(vect, sep=' ')
            vect = Binary(pickle.dumps(vect, protocol=2))
            to_insert.append({
                'idx': len(word2id),
                'item': word,
                'vector': vect
            })
            word2id[word] = len(word2id)

    logger.info('importing...')
    try:
        collection.insert(to_insert)
    except Exception as e:
        logger.error('unexpected error')
        logger.exception(e)

    logger.info('indexing...')
    collection.create_index('idx', unique=True)
    collection.create_index('item', unique=True)


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

    logger.info('db name: %s' % db_name)
    logger.info('collection name: %s' % collection_name)
    client = MongoClient(host=host, port=port)
    logger.info('drop collection')
    client[db_name].drop_collection(collection_name)

    collection = client[db_name][collection_name]
    logger.info('processing...')
    import_vectors(pdata, collection)
    logger.info('done.')

    logger.info(collection)
    logger.info(collection.count())

    client.close()
