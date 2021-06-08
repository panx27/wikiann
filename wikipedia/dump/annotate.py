import os
import sys
import re
import logging
import argparse
import multiprocessing

import ujson as json

from common import utils
from common import wikiann


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def process_one(line, verbose=True):
    d = json.loads(line)
    result = {
        'id': d['id'],
        'title': d['title'],
        'sentences': []
    }
    if d['redirect']:
        return result

    count = {
        'matched_links': 0
    }

    text = d['article']
    sentences = annotator.annotate(text, links=d['links'])
    for sent in sentences:
        matched_links = []
        for link in d['links']:
            if utils.is_in_range((link['start'], link['end']),
                                 (sent['start'], sent['end'])):
                link['start'] -= sent['start']
                link['end'] -= sent['start']
                '''
                # tokenize link text
                link['tokens'] = annotator.tokenize(link['text'], link['start'])
                try:
                    assert link['tokens'][0]['start'] == link['start']
                    assert link['tokens'][-1]['end'] == link['end']
                except AssertionError:
                    if verbose:
                        logger.warning(f"wicked link: ({link}, {d['title']})")
                    continue
                '''
                matched_links.append(link)
                count['matched_links'] += 1
        sent['links'] = matched_links
    result['sentences'] = sentences

    try:
        assert len(d['links']) == count['matched_links']
    except AssertionError:
        if verbose:
            logger.warning(f"{d['id']} {d['title']} got unmatched links, expect"
                           f": {len(d['links'])} got: {count['matched_links']}")
            all_matched_links = set()
            for sent in sentences:
                for link in sent['links']:
                    all_matched_links.add(link['text'])
            links = set([i['text'] for i in d['links']])
            logger.warning(f'unmatched links: {links - all_matched_links}')

    return result


def process_block(inpath, outpath, verbose=True):
    with open(outpath, 'w') as fw, open(inpath, 'r') as f:
        for line in f:
            result = process_one(line, verbose=verbose)
            if result['sentences']:
                fw.write(json.dumps(result, sort_keys=True) + '\n')


def process(inpath, outpath, verbose=True):
    try:
        process_block(inpath, outpath, verbose=verbose)
    except Exception as e:
        logger.error('unexpected error')
        logger.error(inpath)
        logger.exception(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('indir',
                        help='input directory (blocks/)')
    parser.add_argument('outdir',
                        help='output directory')
    parser.add_argument('lang',
                        help='wikipedia language code')
    parser.add_argument('--nworker', '-n', default=1,
                        help='number of processors to use (default=1)')
    parser.add_argument('--verbose', '-v', default=False, action='store_true',
                        help='verbose logging')
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    # Must be a global variable, each worker will make a copy
    annotator = wikiann.get_annotator(args.lang)

    logger.info('processing...')
    pool = multiprocessing.Pool(processes=int(args.nworker))
    logger.info(f'# of workers: {args.nworker}')
    logger.info(f'parent pid: {os.getpid()}')
    for i in sorted(os.listdir(args.indir),
                    key=lambda x: os.path.getsize(f'{args.indir}/{x}'),
                    reverse=True):
        inpath = f'{args.indir}/{i}'
        outpath = f'{args.outdir}/{i}.ann'
        pool.apply_async(process, args=(inpath, outpath, args.verbose,),)
    pool.close()
    pool.join()
    logger.info('done.')
