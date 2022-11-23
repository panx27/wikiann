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


def get_cats(pdata):
    res = defaultdict(set)
    with open(pdata, 'r') as f:
        for line in f:
            d = json.loads(line)
            res[d['title']] = set(d['categories'])
    return dict(res)


def process(pdata):
    try:
        return get_cats(pdata)
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
    res = defaultdict(list)
    for r in results:
        b_cats = r.get()
        for x in b_cats:
            for y in b_cats[x]:
                res[x].append(y)
    print(len(res))
    logger.info('done.')

    logger.info('writing...')
    os.makedirs(args.outdir, exist_ok=True)
    with open(f'{args.outdir}/categories.json', 'w') as fw:
        json.dump(res, fw, indent=4)
    # with open(f'{args.outdir}/categories.jsonl', 'w') as fw:
    #     for i in res:
    #         d = {
    #             'title': i,
    #             'categories': res[i]
    #         }
    #         fw.write(json.dumps(d)+'\n')

    logger.info('done.')
