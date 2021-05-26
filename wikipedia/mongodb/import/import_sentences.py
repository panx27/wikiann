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


def import_sents(pdata, name):
    if username and password:
        client = MongoClient(host=host, port=port,
                             username=username, password=password)
    else:
        client = MongoClient(host=host, port=port)

    collection = client[db_name][collection_name]
    sents = []
    with open(pdata, 'r') as f:
        for line in f:
            d = json.loads(line)
            for n_sent, sent in enumerate(d['sentences']):
                sent['len_links'] = len(sent['links'])

                if langlinks:
                    sent['len_links_ll'] = 0
                    for n, i in enumerate(sent['links']):
                        if i['title'] in langlinks:
                            title_ll, id_ll = langlinks[i['title']]
                            sent['links'][n]['id_ll'] = id_ll
                            sent['links'][n]['title_ll'] = title_ll
                            sent['len_links_ll'] += 1

                sent['id'] = d['id']
                sent['title'] = d['title']
                if sent['title'] in langlinks:
                    title_ll, id_ll = langlinks[sent['title']]
                    sent['id_ll'] = id_ll
                    sent['title_ll'] = title_ll

                sent['_chunk_id'] = name
                sent['_id'] = f'{d["id"]}_{n_sent}'
                sents.append(sent)
    if sents:
        # insert_many is much faster than insert_one,
        # but it requires larger RAM usage,
        # try to reduce the size of the list,
        # if you don't have enough RAM
        collection.insert_many(sents)
    client.close()


def process(pdata, name):
    try:
        import_sents(pdata, name)
    except Exception as e:
        logger.error('unexpected error')
        logger.error(pdata)
        logger.exception(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('indir', help='Input directory (blocks.ann)')
    parser.add_argument('host', help='MongoDB host')
    parser.add_argument('port', help='MongoDB port')
    parser.add_argument('db_name', help='Database name')
    parser.add_argument('collection_name', help='Collection name')
    parser.add_argument('--planglinks', '-l', default=None,
                        help='Path to langlinks mapping')
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
    planglinks = args.planglinks
    username = args.username
    password = args.password

    langlinks = {}
    if 'en' not in db_name or 'en' not in collection_name:
        if not planglinks:
            msg = 'You may want to add langlinks for non-English languages.'
            logger.warning(msg)
    if planglinks:
        count = {
            'duplicate': 0,
        }
        logger.info('loading langlinks...')
        tmp = json.load(open(planglinks))
        for i in tmp:
            if i['title'] in langlinks:
                count['duplicate'] += 1
                continue
            langlinks[i['title']] = (i['title_ll'], i['id_ll'])
        logger.warning(f'# of duplicate langlinks: {count["duplicate"]}')
        logger.info('done.')
        del tmp

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
