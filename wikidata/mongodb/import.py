import sys
import os
import bz2
import logging
import argparse
import multiprocessing
from itertools import islice

import ujson as json
from pymongo import MongoClient


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def process(lines, verbose=False):
    try:
        if verbose:
            pid = os.getpid()
            logger.info(f'{pid} started')
        client = MongoClient(host=host, port=port)
        collection = client[db_name][collection_name]
        data = []
        for line in lines:
            line = line.decode('utf-8').rstrip(',\n')
            if not line:
                continue
            if line == '[' or line == ']':
                continue
            d = json.loads(line)
            d['_id'] = d['id']
            d['properties'] = list(d['claims'].keys())
            data.append(d)
        if data:
            # insert_many is much faster than insert_one,
            # but it requires larger RAM usage, reduce --chunk_size
            # if you don't have enough RAM
            collection.insert_many(data)
        client.close()
        if verbose:
            logger.info(f'{pid} finished')
    except Exception as e:
        logger.exception(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('inpath',
                        help='Path to inpath file (xxxxxxxx-all.json.bz2)')
    parser.add_argument('host', help='MongoDB host')
    parser.add_argument('port', help='MongoDB port')
    parser.add_argument('db_name', help='Database name')
    parser.add_argument('collection_name', help='Collection name')
    parser.add_argument('--chunk_size', '-c', default=10000,
                        help='Chunk size (default=10000, '
                        'RAM usage depends on chunk size)')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    args = parser.parse_args()

    pdata = args.inpath
    host = args.host
    port = int(args.port)
    db_name = args.db_name
    collection_name = args.collection_name
    nworker = int(args.nworker)
    chunk_size = int(args.chunk_size)

    logger.info(f'db name: {db_name}')
    logger.info(f'collection name: {collection_name}')
    client = MongoClient(host=host, port=port)
    logger.info('drop old collection')
    client[db_name].drop_collection(collection_name)

    logger.info('importing...')
    pool = multiprocessing.Pool(processes=nworker)
    logger.info(f'# of workers: {nworker}')
    logger.info(f'chunk size: {chunk_size}')
    logger.info(f'parent pid: {os.getpid()}')
    with bz2.BZ2File(pdata) as f:
        for chunk in iter(lambda: tuple(islice(f, chunk_size)), ()):
            pool.apply_async(process, args=(chunk,),)
    pool.close()
    pool.join()

    logger.info('done.')
