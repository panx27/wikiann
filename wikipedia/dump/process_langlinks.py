import sys
import os
import re
import logging
import gzip
import argparse
from collections import defaultdict

import ujson as json

from utils import wikiid2title


'''
CREATE TABLE `langlinks` (
  `ll_from` int(8) unsigned NOT NULL DEFAULT '0',
  `ll_lang` varbinary(20) NOT NULL DEFAULT '',
  `ll_title` varbinary(255) NOT NULL DEFAULT '',
  UNIQUE KEY `ll_from` (`ll_from`,`ll_lang`),
  KEY `ll_lang` (`ll_lang`,`ll_title`)
)
'''


def normalize_title(title):
    # Remove backslash
    title = title.replace('\\', '')
    # Replace underline
    title = title.replace('_', ' ')
    return title


def langlink_mapping(p_langlink, tgt_lang, outdir, split=True):
    res = defaultdict(list)
    err = []

    logger.info('loading... %s' % p_langlink)
    f = gzip.open(p_langlink, 'rb')

    logger.info('processing...')
    for line in f:
        line = line.decode('utf-8', 'ignore')
        for i in line.strip().split('),('):
            i = i.replace('INSERT INTO `langlinks` VALUES (', '')
            m = re.match('(\d+),\'(\S+)\',\'(.+)\'', i)
            if m != None:
                ll_from = m.group(1)
                ll_lang = m.group(2)
                ll_title = m.group(3)
                if tgt_lang == 'all' or tgt_lang == ll_lang:
                    if ll_from in id2title:
                        res[ll_lang].append({
                            'title': normalize_title(ll_title),
                            'title_ll': normalize_title(id2title[ll_from]['title']),
                            'id_ll': id2title[ll_from]['id']
                        })
                    else:
                        err.append((ll_from, ll_lang, ll_title))

    logger.info('writing...')
    os.makedirs(outdir, exist_ok=True)
    json.dump(res, open('%s/langlinks.json' % outdir, 'w'), indent=4)
    if split:
        os.makedirs('%s/split' % outdir, exist_ok=True)
        for lang in res:
            json.dump(res[lang],
                      open('%s/split/%s.json' % (outdir, lang), 'w'), indent=4)

    with open('%s/langlinks.error.tsv' % outdir, 'w') as fw:
        for ll_from, ll_lang, ll_title in err:
            fw.write('%s\t%s\t%s\n' % (ll_from, ll_lang, ll_title))

    logger.info('done.')


if __name__ == '__main__':
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
    logging.root.setLevel(level=logging.INFO)

    des = 'Process Wikipedia langlinks dump'
    parser = argparse.ArgumentParser(description=des)
    parser.add_argument('langlink', help='path to langlinks.sql.gz')
    parser.add_argument('outdir', help='output dir')
    parser.add_argument('--id2title', help='path to id2title mapping')
    # parser.add_argument('--page', help='path to wiki-page.sql.gz ' \
    #                     '(using page dump to generate id2title)')
    parser.add_argument('--tgtlang', '-t', default='all',
                        help='target language')
    args = parser.parse_args()

    # try:
    #     assert bool(args.id2title) != bool(args.page)
    # except AssertionError:
    #     print('Please select one id2title mapping method')
    #     exit()
    if args.id2title:
        logger.info('loading... %s' % args.id2title)
        id2title = json.load(open(args.id2title))
    # if args.page:
    #     logger.info('loading... %s' % os.path.basename(p_page))
    #     id2title = wikiid2title.get_mapping_id2title_page(p_page)

    langlink_mapping(args.langlink, args.tgtlang, args.outdir)
