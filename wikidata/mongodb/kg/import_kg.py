import sys
import os
import logging
import argparse
import multiprocessing
from itertools import islice

import ujson as json
from pymongo import MongoClient


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def parse_claims(data):
    res = []
    for property_id in data['claims']:
        for i in data['claims'][property_id]:
            try:
                if i['mainsnak']['datatype'] == 'wikibase-item' and \
                   i['mainsnak']['snaktype'] == 'value':  # only keep object that has qid
                    qid = i['mainsnak']['datavalue']['value']['id']
                    res.append((property_id, qid))
            except KeyError:
                logger.error(i)
    return res


def process(ids, verbose=False):
    try:
        if verbose:
            pid = os.getpid()
            logger.info(f'{pid} started')
        if username and password:
            client = MongoClient(host=host, port=port,
                                username=username, password=password)
        else:
            client = MongoClient(host=host, port=port)
        collection = client[db_name][collection_name]
        data = []
        query = {'id': {'$in': [i['id'] for i in ids]}}
        project = {'_id': 0, 'id': 1, 'claims': 1}
        for i in collection.find(query):
            claims = parse_claims(i)
            s = i['id']
            for p, o in claims:
                data.append({
                    's': s,
                    'p': p,
                    'o': o
                })
        if data:
            collection_imex = client[db_name][collection_name_imex]
            collection_imex.insert_many(data)
        client.close()
        if verbose:
            logger.info(f'{pid} finished')
    except Exception as e:
        logger.exception(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('host', help='MongoDB host')
    parser.add_argument('port', help='MongoDB port')
    parser.add_argument('db_name', help='Database name')
    parser.add_argument('collection_name_read', help='Collection name to read')
    parser.add_argument('collection_name_import',
                        help='Collection name to import')
    parser.add_argument('--chunk_size', '-c', default=10000,
                        help='Chunk size (default=10000, '
                        'RAM usage depends on chunk size)')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    parser.add_argument('--username', '-u', default=None,
                        help='Username (if authentication is enabled)')
    parser.add_argument('--password', '-p', default=None,
                        help='Password (if authentication is enabled)')
    args = parser.parse_args()

    host = args.host
    port = int(args.port)
    db_name = args.db_name
    collection_name = args.collection_name_read
    nworker = int(args.nworker)
    chunk_size = int(args.chunk_size)
    collection_name_imex = args.collection_name_import
    username = args.username
    password = args.password

    if username and password:
        client = MongoClient(host=host, port=port,
                             username=username, password=password)
    else:
        client = MongoClient(host=host, port=port)

    logger.info(f'db name: {db_name}')
    logger.info(f'collection to read: {collection_name}')
    collection = client[db_name][collection_name]
    logger.info(f'collection to import: {collection_name_imex}')
    logger.info('drop old collection')
    client[db_name].drop_collection(collection_name_imex)

    logger.info('importing...')
    pool = multiprocessing.Pool(processes=nworker)
    logger.info(f'# of workers: {nworker}')
    logger.info(f'chunk size: {chunk_size}')
    logger.info(f'parent pid: {os.getpid()}')

    # query = {'sitelinks.enwiki.title': {'$regex': '.+'}}
    query = {'id': {'$regex': '.+'}}
    logger.info(f'# of entries: {collection.count_documents(query)}')
    res = collection.find(query, {'_id': 0, 'id': 1})

    for chunk in iter(lambda: tuple(islice(res, chunk_size)), ()):
        pool.apply_async(process, args=(chunk,),)
    pool.close()
    pool.join()

    logger.info('indexing...')
    collection_imex = client[db_name][collection_name_imex]
    logger.info('indexing: s')
    collection_imex.create_index('s')
    logger.info('indexing: p')
    collection_imex.create_index('p')
    logger.info('indexing: o')
    collection_imex.create_index('o')
    logger.info('indexing: {s: 1, o:1}')
    collection_imex.create_index([('s', 1), ('o', 1)])

    logger.info('done.')
