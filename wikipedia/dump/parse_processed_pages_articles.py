import os
import sys
import re
import logging
import argparse
import multiprocessing

import ujson as json

from utils.wikiparser import WikiParser


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def is_in_range(x, y):
    return x[0] >= y[0] and x[1] <= y[1]


def is_overlapping(x, y):
    return max(x[0], y[0]) < min(x[1], y[1])


def normalise_wikilink(s, prefix):
    s = s.replace(' ', '_').strip('_').strip()
    return prefix + s


def process_line(line, wikiparser, verbose=True):
    d = json.loads(line)
    res = {
        'id': d['id'],
        'title': d['title'],
        'sentences': []
    }
    if d['redirect']:
        return res

    count = {
        'matched_links': 0
    }

    text = d['article']
    sentences = wikiparser.parse(text, links=d['links'])
    for sent in sentences:
        matched_links = []
        for link in d['links']:
            if is_in_range((link['start'], link['end']),
                           (sent['start'], sent['end'])):
                link['start'] -= sent['start']
                link['end'] -= sent['start']
                link['tokens'] = wikiparser.tokenizer(link['text'],
                                                  link['start'])
                try:
                    assert link['tokens'][0]['start'] == link['start']
                    assert link['tokens'][-1]['end'] == link['end']
                except AssertionError:
                    if verbose:
                        msg = 'wicked link: (%s, %s)' % (link, d['title'])
                        logger.warning(msg)
                    continue
                matched_links.append(link)
                count['matched_links'] += 1
        sent['links'] = matched_links
    res['sentences'] = sentences

    try:
        assert len(d['links']) == count['matched_links']
    except AssertionError:
        if verbose:
            msg = 'unmatched links: %s %s. expect: %s got: %s' % \
                (d['id'], d['title'], len(d['links']), count['matched_links'])
            logger.warning(msg)
            all_matched_links = set()
            for sent in sentences:
                for link in sent['links']:
                    all_matched_links.add(link['text'])
            links = set([i['text'] for i in d['links']])
            msg = 'unmatched links: %s' % (links - all_matched_links)
            logger.warning(msg)

    return res


def process_block(inpath, outpath, verbose=True):
    try:
        with open(outpath, 'w') as fw:
            with open(inpath, 'r') as f:
                for line in f:
                    res = process_line(line, wikiparser, verbose=verbose)
                    if res['sentences']:
                        fw.write('%s\n' % json.dumps(res, sort_keys=True))
                    del res

    except Exception:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        msg = 'unexpected error: %s | %s | %s' % \
            (exc_type, exc_obj, exc_tb.tb_lineno)
        logger.error(msg)
        logger.error(inpath)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('indir',
                        help='Input directory (blocks)')
    parser.add_argument('outdir',
                        help='Output directory')
    parser.add_argument('lang',
                        help='Language code')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    parser.add_argument('--verbose', '-v', default=False, action='store_true',
                        help='Verbose logger')
    args = parser.parse_args()

    nworker = int(args.nworker)
    os.makedirs(args.outdir, exist_ok=True)
    wikiparser = WikiParser(args.lang) # It need to be a global variable
                                       # for multiprocessing

    logger.info('processing...')
    pool = multiprocessing.Pool(processes=nworker)
    logger.info('# of workers: %s' % nworker)
    logger.info('parent pid: %s' % os.getpid())
    for i in sorted(os.listdir(args.indir),
                    key=lambda x: os.path.getsize('%s/%s' % (args.indir, x)),
                    reverse=True):
        inpath = '%s/%s' % (args.indir, i)
        outpath = '%s/%s.pp' % (args.outdir, i)
        pool.apply_async(process_block, args=(inpath, outpath, args.verbose,),)
    pool.close()
    pool.join()
    logger.info('done.')
