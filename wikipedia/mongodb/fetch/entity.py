import sys
import os
import logging
import argparse
from collections import defaultdict
import multiprocessing
import subprocess

import ujson as json
from pymongo import MongoClient

import utils


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def get_sent_entity(lang, ins, entities):
    _id = 'title' if lang == 'en' else 'title_ll' # English title
    sent = []
    target_entities = []
    for link in ins['links']:
        if _id in link and link[_id] in entities:
            for n, tok in enumerate(link['tokens']):
                tag = 'B' if n == 0 else 'I'
                target_entities.append((tok, tag))
    for tok in ins['tokens']:
        overlap = False
        offset_tok = (tok['start'], tok['end'])
        for ent in target_entities:
            offset_ent = (ent[0]['start'], ent[0]['end'])
            if utils.is_overlapping(offset_tok, offset_ent):
                overlap = True
                break
        if not overlap:
            tag = 'O'
            sent.append((tok, tag))
    sent += target_entities
    sent = sorted(sent, key=lambda x: x[0]['start'])
    return sent


def process(collection_name, lang, entities, index, outdir):
    try:
        client = MongoClient(host=host, port=port)
        collection = client[db_name][collection_name]

        chunk_entities = entities[index]
        outpath = '%s/%s-%s.tmp' % (outdir, index.start, index.stop)
        with open(outpath, 'w') as fw:
            for ent in chunk_entities:
                if lang == 'en':
                    query = {'titles': {'$in': [ent]}}
                else:
                    query = {'titles_ll': {'$in': [ent]}}
                sents = []
                for i in collection.find(query):
                    sent = get_sent_entity(lang, i, chunk_entities)
                    sents.append(sent)
                if sents:
                    for sent in sents:
                        line = '\n'.join(['%s %s %s-%s' % (tok['text'],
                                                           tag,
                                                           tok['start'],
                                                           tok['end']) \
                                          for tok, tag in sent])
                        fw.write(line + '\n\n')

    except Exception as e:
        logger.error('unexpected error')
        logger.exception(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('host', help='MongoDB host')
    parser.add_argument('port', help='MongoDB port')
    parser.add_argument('db_name', help='Database name')
    parser.add_argument('collection_name', help='Collection name')
    parser.add_argument('lang', help='Language')
    parser.add_argument('target_entities',
                        help='Path to target entities (Wikipedia titles)')
    parser.add_argument('outdir', help='Output directory')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    args = parser.parse_args()

    host = args.host
    port = int(args.port)
    client = MongoClient(host=host, port=port)
    db_name = args.db_name

    target_entities = []
    with open(args.target_entities, 'r') as f:
        for line in f:
            target_entities.append(line.strip().replace('_', ' '))
    os.makedirs(args.outdir, exist_ok=True)

    nworker = int(args.nworker)
    pool = multiprocessing.Pool(processes=nworker)
    logger.info('# of workers: %s' % nworker)
    logger.info('# of target entities: %s' % len(target_entities))
    chunk_size = int(len(target_entities) / nworker)
    if chunk_size == 0:
        chunk_size = 1
    logger.info('chunk size: %s' % chunk_size)
    chunks = []
    for i in range(0, len(target_entities), chunk_size):
        chunks.append(slice(i, i+chunk_size))

    logger.info('processing...')
    for i in chunks:
        a = (args.collection_name, args.lang, target_entities, i, args.outdir)
        pool.apply_async(process, args=a,)
    pool.close()
    pool.join()

    logger.info('merging...')
    cmds = [
        'cat %s/*.tmp > %s/output.bio' % (args.outdir, args.outdir),
        'rm %s/*.tmp' % (args.outdir),
    ]
    for cmd in cmds:
        subprocess.call(cmd, shell=True)
