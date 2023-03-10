import sys
import os
import logging
from collections import defaultdict
import multiprocessing
import subprocess
import argparse
import pprint
from itertools import islice

from pymongo import MongoClient
import ujson as json

import utils
from encoder import Encoder


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def get_original(ins):
    if ins['tokens'] == []:
        return ''
    chars = list(ins['tokens'][-1]['end'] * ' ')
    for tok in ins['tokens']:
        chars[tok['start']:tok['end']] = tok['text']
    return ''.join(chars)


def get_sent_plain(ins, lower=False, encoder=None):
    sent = [t['text'] for t in ins['tokens']]
    if encoder:
        sent = [encoder.encoding(t) for t in sent]
    if lower:
        sent = [t.lower() for t in sent]
    return sent


def get_sent(lang, ins, ll_lang=None, lower=True, encoder=None,
             keep_sent_wo_link=False, keep_link_wo_ll=False, html=False):
    sent = []
    links = []
    for link in ins['links']:
        offset_ent = (link['start'], link['end'])
        if not ll_lang:
            ent = utils.convert_link(lang, link, html=html)
        else:
            ent = utils.convert_link_ll(lang, ll_lang, link,
                                        keep_link_wo_ll=keep_link_wo_ll,
                                        html=html)
        if not ent:
            continue
        links.append((ent, offset_ent))

    if not links and not keep_sent_wo_link:
        return []

    for tok in ins['tokens']:
        offset_tok = (tok['start'], tok['end'])
        overlap = False
        for ent, offset_ent in links:
            if utils.is_overlapping(offset_tok, offset_ent):
                overlap = True
                break
        if not overlap and utils.is_valid_tok(tok['text']):
            tok_text = tok['text']
            if lower:
                tok_text = tok_text.lower()
            if encoder:
                tok_text = encoder.encoding(tok_text)
            sent.append((tok_text, offset_tok))
    sent += links
    sent = [t for t, o in sorted(sent, key=lambda x: x[1][0])]
    return sent


def get_sents_one_link_per_sent(lang, ins, lower=True, encoder=None):
    for link in ins['links']:
        offset_ent = (link['start'], link['end'])
        sent = []
        ent = utils.convert_link(lang, link)
        if not ent:
            continue
        for tok in ins['tokens']:
            offset_tok = (tok['start'], tok['end'])
            if not utils.is_overlapping(offset_tok, offset_ent) and \
               utils.is_valid_tok(tok['text']):
                tok_text = tok['text']
                if lower:
                    tok_text = tok_text.lower()
                if encoder:
                    tok_text = encoder.encoding(tok_text)
                sent.append((tok_text, offset_tok))
        sent += [(ent, offset_ent)]
        sent = [t for t, o in sorted(sent, key=lambda x: x[1][0])]
        yield sent


def process(collection_name, lang, chunk_id, outdir,
            plain=False, one_link_per_sent=False,
            ll_lang=None, keep_link_wo_ll=False, keep_sent_wo_link=False):
    try:
        if username and password:
            client = MongoClient(host=host, port=port,
                                 username=username, password=password)
        else:
            client = MongoClient(host=host, port=port)
        collection = client[db_name][collection_name]
        encoder = Encoder(lang) if lang in Encoder.langs else None

        with open('%s/%s.tmp' % (outdir, chunk_id), 'w') as fw:
            for i in collection.find({'_chunk_id': chunk_id}):
                if plain:
                    sents = [get_sent_plain(i, encoder=encoder)]
                elif one_link_per_sent:
                    sents = get_sents_one_link_per_sent(lang, i,
                                                        encoder=encoder)
                else:
                    sents = [get_sent(lang, i, encoder=encoder, ll_lang=ll_lang,
                                     keep_sent_wo_link=keep_sent_wo_link,
                                     keep_link_wo_ll=keep_link_wo_ll)]
                for sent in sents:
                    if not plain and len(sent) <= 1:
                        continue
                    fw.write('%s\n' % ' '.join(sent))
        client.close()

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
    parser.add_argument('outdir', help='Output directory')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    parser.add_argument('--plain', action='store_true',
                        help='FORMAT: plain text')
    parser.add_argument('--one_link_per_sent', action='store_true',
                        help='FORMAT: One link per sentence')
    parser.add_argument('--ll_lang', default=None,
                        help='Language of langlinks (ll)')
    parser.add_argument('--keep_sent_wo_link', action='store_true',
                        help='Keep sentences wo link')
    parser.add_argument('--keep_link_wo_ll', action='store_true',
                        help='Keep links wo langlink')
    parser.add_argument('--username', '-u', default=None,
                        help='Username (if authentication is enabled)')
    parser.add_argument('--password', '-p', default=None,
                        help='Password (if authentication is enabled)')
    args = parser.parse_args()

    host = args.host
    port = int(args.port)
    client = MongoClient(host=host, port=port)
    db_name = args.db_name
    nworker = int(args.nworker)
    username = args.username
    password = args.password

    os.makedirs(args.outdir, exist_ok=True)
    if not args.plain and not args.one_link_per_sent:
        logger.info('FORMAT: infusing links into sentences')
        logger.info('  ll_lang: %s' % args.ll_lang)
        logger.info('  keep_link_wo_ll: %s' % args.keep_link_wo_ll)
        logger.info('  keep_sent_wo_link: %s' % args.keep_sent_wo_link)
    else:
        assert args.plain != args.one_link_per_sent
        if args.plain:
            logger.info('FORMAT: plain text')
        if args.one_link_per_sent:
            logger.info('FORMAT: One link per sentence')

    if username and password:
        client = MongoClient(host=host, port=port,
                             username=username, password=password)
    else:
        client = MongoClient(host=host, port=port)
    logger.info('# of workers: %s' % nworker)
    chunks = client[db_name][args.collection_name].distinct('_chunk_id')
    logger.info('# of chunks: %s' % len(chunks))
    logger.info('processing...')
    pool = multiprocessing.Pool(processes=nworker)
    for i in chunks:
        a = (args.collection_name, args.lang, i, args.outdir,
             args.plain, args.one_link_per_sent,
             args.ll_lang, args.keep_link_wo_ll, args.keep_sent_wo_link)
        pool.apply_async(process, args=a,)
    pool.close()
    pool.join()

    logger.info('merging...')
    cmds = [
        'cat %s/*.tmp > %s/%s.txt' % (args.outdir, args.outdir, args.lang),
        'rm %s/*.tmp' % (args.outdir),
    ]
    for cmd in cmds:
        subprocess.call(cmd, shell=True)

    json.dump(vars(args), open('%s/args.json' % args.outdir, 'w'))
