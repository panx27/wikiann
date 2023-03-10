import sys
import os
import re
import logging

from unidecode import unidecode
from pymongo import MongoClient
import ujson as json


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def import_dict(pdata, parser, source, collection, priority=1, upsert=False):
    logger.info('loading data... %s' % pdata)
    data = _dict_parser[parser](pdata)

    logger.info('importing...')
    bulk = collection.initialize_unordered_bulk_op()
    for lemma, lemma_lower, gloss, pos in data:
        if lemma == gloss:
            continue
        ins = {
            'lemma': lemma,
            'lemma_lower': lemma_lower,
            'gloss': gloss,
            'pos': pos,
            'source': source,
            'priority': priority,
        }
        if upsert:
            bulk.find(ins).upsert().update({'$set': ins})
        else:
            bulk.insert(ins)
    try:
        res = bulk.execute()
    except Exception as e:
        logger.error('unexpected error')
        logger.exception(e)

    collection.create_index('lemma')
    collection.create_index('lemma_lower')
    logger.info('{} - {}'.format(source, res))


def _json_parser(pdata):
    data = json.load(open(pdata))
    for i in data:
        lemma = i['title']
        gloss = i['title_ll']
        lemma_lower = unidecode(lemma.lower())
        yield lemma, lemma_lower, gloss, ''


def _tsv_parser(pdata):
    with open(pdata, 'r') as f:
        for line in f:
            try:
                lemma, gloss = line.rstrip('\n').split('\t')[:2]
                lemma_lower = unidecode(lemma.lower())
                yield lemma, lemma_lower, gloss, ''
            except:
                msg = 'skip bad line: %s' % (repr(line))
                logger.warn(msg)


_dict_parser = {
    'json': _json_parser,
    'tsv': _tsv_parser,
}


if __name__ == '__main__':
    host = '0.0.0.0'
    port = 12180
    client = MongoClient(host=host, port=port)

    # langlinks
    db_name = 'dict'
    client.drop_database(db_name)
    indir = '/nas/data/m1/panx2/data/KBs/dump/wikipedia/20180401/enwiki-20180401/langlinks/split/'
    for i in sorted(os.listdir(indir)):
        lang = i.replace('.json', '')
        client[db_name].drop_collection(lang)
        collection = client[db_name][lang]
        import_dict('%s/%s' % (indir, i), 'json', 'langlinks', collection)
