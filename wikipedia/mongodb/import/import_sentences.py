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
    client = MongoClient(host=host, port=port)
    collection = client[db_name][collection_name]
    sents = []
    with open(pdata, 'r') as f:
        for line in f:
            d = json.loads(line)
            # In order to create sparse index, do not add empty field
            # e.g., do not add the following
            # sent['links'][n]['id_ll'] = None
            for n_sent, sent in enumerate(d['sentences']):
                ids = set()
                titles = set()
                ids_ll = set()
                titles_ll = set()
                for n, i in enumerate(sent['links']):
                    if i['id']:
                        ids.add(i['id'])
                        titles.add(i['title'])
                    if langlinks and i['title'] in langlinks:
                        title_ll, id_ll = langlinks[i['title']]
                        sent['links'][n]['id_ll'] = id_ll
                        sent['links'][n]['title_ll'] = title_ll
                        ids_ll.add(id_ll)
                        titles_ll.add(title_ll)
                sent['ids_len'] = 0
                if ids:
                    sent['ids'] = list(ids)
                    sent['ids_len'] = len(ids)
                sent['ids_ll_len'] = 0
                if ids_ll:
                    sent['ids_ll'] = list(ids_ll)
                    sent['ids_ll_len'] = len(ids_ll)
                if titles:
                    sent['titles'] = list(titles)
                if titles_ll:
                    sent['titles_ll'] = list(titles_ll)
                sent['source_id'] = d['id']
                sent['source_title'] = d['title']
                if sent['source_title'] in langlinks:
                    title_ll, id_ll = langlinks[sent['source_title']]
                    sent['source_id_ll'] = id_ll
                    sent['source_title_ll'] = title_ll
                sent['_chunk_id'] = name
                sent['_id'] = '%s_%s' % (d['id'], n_sent)
                words = [x['text'].lower() for x in sent['tokens']]
                sent['keywords'] = list(set(words))
                sents.append(sent)
    if sents:
        # insert_many is much faster than insert_one,
        # but it requires larger RAM usage, reduce the size of `sents`
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
    parser.add_argument('indir', help='Input directory (blocks.pp)')
    parser.add_argument('host', help='MongoDB host')
    parser.add_argument('port', help='MongoDB port')
    parser.add_argument('db_name', help='Database name')
    parser.add_argument('collection_name', help='Collection name')
    parser.add_argument('--planglinks', '-p', default=None,
                        help='Path to langlinks mapping')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    args = parser.parse_args()

    indir = args.indir
    host = args.host
    port = int(args.port)
    nworker = int(args.nworker)
    db_name = args.db_name
    collection_name = args.collection_name
    planglinks = args.planglinks

    count = {
        'duplicate': 0,
    }
    langlinks = {}
    if collection_name != 'en':
        assert planglinks
        logger.info('loading langlinks...')
        tmp = json.load(open(planglinks))
        for i in tmp:
            if i['title'] in langlinks:
                count['duplicate'] += 1
                continue
            langlinks[i['title']] = (i['title_ll'], i['id_ll'])
        logger.warning('# of duplicate langlinks: %s' % (count['duplicate']))
        logger.info('done.')
        del tmp

    logger.info('db name: %s' % db_name)
    logger.info('collection name: %s' % collection_name)
    logger.info('drop collection')
    client = MongoClient(host=host, port=port)
    client[db_name].drop_collection(collection_name)

    logger.info('importing...')
    pool = multiprocessing.Pool(processes=nworker)
    logger.info('# of workers: %s' % nworker)
    for i in sorted(os.listdir(indir),
                    key=lambda x: os.path.getsize('%s/%s' % (indir, x)),
                    reverse=True):
        inpath = '%s/%s' % (indir, i)
        pool.apply_async(process, args=(inpath, i),)
    pool.close()
    pool.join()

    logger.info('done.')
