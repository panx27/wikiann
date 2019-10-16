import sys
import os
import re
import logging
import gzip
import argparse
from collections import defaultdict

import ujson as json


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


''' From langlinks.sql.gz

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


def process(p_langlink, tgt_lang, id2title, outdir, split=True):
    mapping = defaultdict(list)
    errors = []

    logger.info(f'loading {p_langlink}')
    logger.info('processing...')
    with gzip.open(p_langlink, 'rb') as f:
        for line in f:
            line = line.decode('utf-8', 'ignore')
            for i in line.strip().split('),('):
                i = i.replace('INSERT INTO `langlinks` VALUES (', '')
                m = re.match(r"(\d+),'(\S+)','(.+)'", i)
                if m:
                    ll_from = m.group(1)
                    ll_lang = m.group(2)
                    ll_title = m.group(3)
                    if tgt_lang == 'all' or tgt_lang == ll_lang:
                        if ll_from in id2title:
                            ins = {
                                'title': normalize_title(ll_title),
                                'title_ll': normalize_title(
                                    id2title[ll_from]['title']),
                                'id_ll': id2title[ll_from]['id']
                            }
                            mapping[ll_lang].append(ins)
                        else:
                            errors.append((ll_from, ll_lang, ll_title))

    logger.info('writing...')
    os.makedirs(outdir, exist_ok=True)
    outpath = f'{outdir}/langlinks.json'
    with open(outpath, 'w') as fw:
        json.dump(mapping, fw, indent=4)
    if split:
        os.makedirs(f'{outdir}/split', exist_ok=True)
        for lang in mapping:
            outpath = f'{outdir}/split/{lang}.json'
            with open(outpath, 'w') as fw:
                json.dump(mapping[lang], fw, indent=4)
    with open(f'{outdir}/langlinks.error.tsv', 'w') as fw:
        for ll_from, ll_lang, ll_title in errors:
            fw.write(f'{ll_from}\t{ll_lang}\t{ll_title}\n')
    logger.info('done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('langlink', help='path to langlinks.sql.gz')
    parser.add_argument('outdir', help='output directory')
    parser.add_argument('id2title',
                        help='path to id2title mapping (id2title.json)')
    parser.add_argument('--tgtlang', '-t', default='all',
                        help='target language')
    args = parser.parse_args()

    logger.info(f'loading {args.id2title}')
    id2title = json.load(open(args.id2title))

    process(args.langlink, args.tgtlang, id2title, args.outdir)
