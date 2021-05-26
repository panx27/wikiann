import sys
import os
import logging
import multiprocessing
import argparse

from pymongo import MongoClient
import ujson as json


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def import_pages(pdata, name):
    if username and password:
        client = MongoClient(host=host, port=port,
                             username=username, password=password)
    else:
        client = MongoClient(host=host, port=port)
    collection = client[db_name][collection_name]

    pages = []
    with open(pdata, 'r') as f:
        for line in f:
            d = json.loads(line)
            d['_chunk_id'] = name
            d['_id'] = d['id']
            pages.append(d)
    if pages:
        # insert_many is much faster than insert_one,
        # but it requires larger RAM usage,
        # try to reduce the size of the list,
        # if you don't have enough RAM
        collection.insert_many(pages)
    client.close()


def process(pdata, name):
    try:
        import_pages(pdata, name)
    except Exception as e:
        logger.error('unexpected error')
        logger.error(pdata)
        logger.exception(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('indir', help='Input directory (blocks)')
    parser.add_argument('host', help='MongoDB host')
    parser.add_argument('port', help='MongoDB port')
    parser.add_argument('db_name', help='Database name')
    parser.add_argument('collection_name', help='Collection name')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    parser.add_argument('--username', '-u', default=None,
                        help='Username (if authentication is enabled)')
    parser.add_argument('--password', '-p', default=None,
                        help='Password (if authentication is enabled)')
    args = parser.parse_args()

    indir = args.indir
    host = args.host
    port = int(args.port)
    nworker = int(args.nworker)
    db_name = args.db_name
    collection_name = args.collection_name
    username = args.username
    password = args.password

    logger.info(f'db name: {db_name}')
    logger.info(f'collection name: {collection_name}')
    if username and password:
        client = MongoClient(host=host, port=port,
                             username=username, password=password)
    else:
        client = MongoClient(host=host, port=port)

    logger.info('drop old collection')
    client[db_name].drop_collection(collection_name)

    logger.info('importing...')
    pool = multiprocessing.Pool(processes=nworker)
    logger.info(f'# of workers: {nworker}')
    for i in sorted(os.listdir(indir),
                    key=lambda x: os.path.getsize(f'{indir}/{x}'),
                    reverse=True):
        inpath = f'{indir}/{i}'
        pool.apply_async(process, args=(inpath, i),)
    pool.close()
    pool.join()

    logger.info('done.')
