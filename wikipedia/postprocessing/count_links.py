import sys
import os
import re
import argparse
import logging
from collections import Counter
from collections import defaultdict
from copy import deepcopy
import multiprocessing

import ujson as json
import jellyfish._jellyfish as jf


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


RE_STRIP = r' \([^)]*\)|\<[^)]*\>|,|"|\.|\'|:|-'
STOP_WORDS = ['a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
              'has', 'he', 'i', 'in', 'is', 'it', 'its', 'of', 'on', 'that',
              'the', 'their', 'we', 'to', 'was', 'were', 'with', 'you', 'your',
              'yours', 'our', 'ours', 'theirs', 'her', 'hers', 'his', 'him',
              'mine', 'or', 'but', 'though', 'since']


def strip_mention(text):
    text = text.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
    text = text.lower().strip()
    text = text.replace('\\', '')
    text = ' '.join(text.split())
    return text


def expand_mention(text):
    res = []
    # Strip mention
    res.append(' '.join(re.sub(RE_STRIP, '', text).strip().split()))
    # # Remove spaces
    # res.append(re.sub(RE_STRIP, '', text).replace(' ', '').strip())
    # Remove stop words
    res.append(' '.join([word for word in text.split() \
                         if word not in STOP_WORDS]).strip())
    return res


def filter_title(title):
    if title == '':
        return False
    if title.startswith('File:'):
        return False
    if title.startswith('Image:'):
        return False
    return True


def filter_mention(mention):
    if not mention:
        return False
    if mention == '':
        return False
    return True


def count_links(pdata, fuzzy=False):
    count = {
        'num_of_links': 0,
        'num_of_invalid_title': 0,
    }
    res = defaultdict(lambda: defaultdict(int))
    with open(pdata, 'r') as f:
        for line in f:
            d = json.loads(line)
            for link in d['links']:
                count['num_of_links'] += 1
                if not link['id']:
                    count['num_of_invalid_title'] += 1
                    continue
                # title = link['title'].replace(' ', '_')
                title = link['title']
                if not filter_title(title):
                    count['num_of_invalid_title'] += 1
                    continue

                # Expand mention
                mention = strip_mention(link['text'])
                mentions = [mention]
                mentions += expand_mention(mention)
                mentions = set(mentions)

                for mention in mentions:
                    if not filter_mention(mention):
                        continue
                    if fuzzy:
                        mention = jf.nysiis(mention)
                    res[mention][title] += 1
    return dict(res), count


def process(pdata, fuzzy=False):
    try:
        return count_links(pdata, fuzzy=fuzzy)
    except Exception as e:
        logger.error('unexpected error')
        logger.exception(e)


def filter_low_frequent_entities(data, threshold):
    logger.info(f'filter low frequent entities, threshold={threshold}')
    logger.info(f'# of mentions (before): {len(data)}')
    res = {}
    for mention in data:
        r = {k: v for k, v in data[mention].items() if v > threshold}
        if r:
            res[mention] = r
    logger.info(f'# of mentions (after): {len(res)}')
    return res


def get_kbid2mention(data):
    res = defaultdict(lambda: defaultdict(int))
    for mention in data:
        for kbid in data[mention]:
            assert type(data[mention][kbid]) == int
            res[kbid][mention] = data[mention][kbid]
    return res


def get_entity_prior(data):
    res = {}
    for kbid in data:
        res[kbid] = sum(Counter(data[kbid]).values())
    tol = sum(Counter(res).values())
    for kbid in res:
        res[kbid] /= tol
    return res


def add_score(data):
    for mention in data:
        c = Counter(data[mention])
        tol = sum(c.values())
        assert type(tol) == int
        for kbid in data[mention]:
            data[mention][kbid] = data[mention][kbid] / tol


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('indir', help='input dir (blocks/)')
    parser.add_argument('outdir', help='output dir')
    parser.add_argument('--fuzzy', '-f', default=False,
                        help='generate fuzzy mention table')
    parser.add_argument('--threshold', '-t', default=0,
                        help='threshold')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    args = parser.parse_args()

    logger.info('counting...')
    mention2kbid = defaultdict(lambda: defaultdict(int))
    count = defaultdict(int)
    nworker = int(args.nworker)
    pool = multiprocessing.Pool(processes=nworker)
    logger.info(f'# of workers: {nworker}')
    logger.info('processing...')
    results = [] # TO-DO: occupied too large RAM
    for i in os.listdir(args.indir):
        _args = (f'{args.indir}/{i}', args.fuzzy,)
        results.append(pool.apply_async(process, args=_args,))
    pool.close()
    pool.join()

    logger.info('merging...')
    for r in results:
        b_mention2kbid, b_count = r.get()
        for x in b_mention2kbid:
            for y in b_mention2kbid[x]:
                mention2kbid[x][y] += b_mention2kbid[x][y]
        for x in b_count:
            count[x] += b_count[x]
    logger.info('done.')

    for i in count:
        logger.info(f'{i}: {count[i]}')

    threshold = int(args.threshold)
    if threshold > 0:
        mention2kbid = filter_low_frequent_entities(mention2kbid, threshold)

    logger.info('converting kbid2mention...')
    kbid2mention = get_kbid2mention(mention2kbid)
    logger.info('done.')

    os.makedirs(args.outdir, exist_ok=True)
    logger.info('computing entity prior...')
    entity_prior = get_entity_prior(kbid2mention)
    with open(f'{args.outdir}/entity_prior.json', 'w') as fw:
        json.dump(entity_prior, fw, indent=4)
    logger.info('done.')

    logger.info('computing mention2kbid...')
    with open(f'{args.outdir}/mention2kbid_raw.json', 'w') as fw:
        json.dump(mention2kbid, fw, indent=4)
    add_score(mention2kbid)
    with open(f'{args.outdir}/mention2kbid.json', 'w') as fw:
        json.dump(mention2kbid, fw, indent=4)
    logger.info('done.')

    logger.info('computing kbid2mention...')
    with open(f'{args.outdir}/kbid2mention_raw.json', 'w') as fw:
        json.dump(kbid2mention, fw, indent=4)
    add_score(kbid2mention)
    with open(f'{args.outdir}/kbid2mention.json', 'w') as fw:
        json.dump(kbid2mention, fw, indent=4)
    logger.info('done.')
