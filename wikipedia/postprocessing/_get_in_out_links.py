import sys
import os
import re
import argparse
import logging
from collections import defaultdict
from copy import deepcopy
import multiprocessing

import ujson as json


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def get_links(pdata):
    count = {
        'num_of_links': 0,
        'num_of_invalid_title': 0,
    }
    outlinks = defaultdict(set)
    inlinks = defaultdict(set)
    with open(pdata, 'r') as f:
        for line in f:
            d = json.loads(line)
            for link in d['links']:
                count['num_of_links'] += 1
                if not link['id']:
                    count['num_of_invalid_title'] += 1
                    continue
                outlinks[d['title']].add(link['title'])
                inlinks[link['title']].add(d['title'])
    return dict(outlinks), dict(inlinks), count


def process(pdata):
    try:
        return get_links(pdata)
    except Exception as e:
        logger.error('unexpected error')
        logger.exception(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('indir', help='input dir (blocks/)')
    parser.add_argument('outdir', help='output dir')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    args = parser.parse_args()

    logger.info('counting...')
    nworker = int(args.nworker)
    pool = multiprocessing.Pool(processes=nworker)
    logger.info(f'# of workers: {nworker}')
    logger.info('processing...')
    results = [] # TO-DO: occupied too large RAM
    for i in os.listdir(args.indir):
        _args = (f'{args.indir}/{i}',)
        results.append(pool.apply_async(process, args=_args,))
    pool.close()
    pool.join()

    logger.info('merging...')
    outlinks = defaultdict(set)
    inlinks = defaultdict(set)
    count = defaultdict(int)
    for r in results:
        b_outlinks, b_inlinks, b_count = r.get()
        for x in b_outlinks:
            for y in b_outlinks[x]:
                outlinks[x].add(y)
        for x in b_count:
            count[x] += b_count[x]
    logger.info('done.')
    for i in count:
        logger.info(f'{i}: {count[i]}')
    del results

    logger.info('writing.')
    os.makedirs(args.outdir, exist_ok=True)
    with open(f'{args.outdir}/outlinks.json', 'w') as fw:
        json.dump(outlinks, fw, indent=4)
    with open(f'{args.outdir}/inlinks.json', 'w') as fw:
        json.dump(inlinks, fw, indent=4)
    logger.info('done.')
