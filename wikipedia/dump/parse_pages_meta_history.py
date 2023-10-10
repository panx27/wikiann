import sys
import os
import io
import re
import bz2
import argparse
import logging
import multiprocessing
import subprocess

from lxml import etree
from xml.etree.cElementTree import iterparse, dump
import ujson as json

from common import wikimarkup
from common.extract import Extractor
from common.utils import (
    replace_links,
    extract_sections,
    extract_categories,
    extract_infobox
)


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)

disambiguation_page_patterns = [
    re.compile(r'({{disambiguation.*?}})', re.I),
    re.compile(r'({{.*?disambiguation}})', re.I),
    re.compile(r'({{hndis.*?}})', re.I)
]
tag_patterns = re.compile(b'(.*?)<(/?\w+)[^>]*>(?:([^<]*)(<.*?>)?)?')


def fast_iter(beg, end, input_path, output_path):
    # Using index to seek bz2
    bz2f = bz2.BZ2File(input_path)
    bz2f.seek(beg)
    if end == -1:
        chunks = bz2f.read(-1)
    else:
        chunks = bz2f.read(end - beg)
    # Wrap up XML string
    chunks = b'<pages>' + chunks + b'</pages>'

    with open(f'{output_path}', 'w') as fw:
        elems = etree.iterparse(io.BytesIO(chunks), events=('end',), tag='page')
        for _, elem in elems:
            ns = elem.find('ns').text
            if ns != '0':  # Main page (ns == 0) only
                continue

            res = {
                'id': elem.find('id').text,
                'title': elem.find('title').text
            }

            # Redirect
            if elem.find('redirect') is not None:
                res['redirect'] = elem.find('redirect').attrib['title']
            else:
                res['redirect'] = None

            extractor = Extractor(res['id'], 0, '', res['title'], [])
            res['revisions'] = []
            for i in elem.findall('revision'):
                rev = {
                    'id': i.find('id').text,
                    'timestamp': i.find('timestamp').text,
                }
                try:
                    rev['contributor'] = {'id': i.find('contributor').find('id').text}
                except AttributeError:
                    rev['contributor'] = None
                try:
                    rev['comment'] = i.find('comment').text
                except AttributeError:
                    rev['comment'] = None
                raw_markup = i.find('text').text
                if raw_markup is None:
                    raw_markup = ''

                paragraphs = extractor.clean_text(raw_markup,
                                                mark_headers=True,
                                                expand_templates=False,
                                                html_safe=False)
                plain_text, links, elinks = replace_links('\n'.join(paragraphs))

                rev['sections'] = extract_sections(plain_text)
                rev['categories'] = extract_categories(raw_markup)
                rev['infobox'] = extract_infobox(raw_markup)
                rev['article'] = plain_text
                rev['links'] = links
                rev['external_links'] = elinks

                res["revisions"].append(rev)
            fw.write(f'{json.dumps(res)}\n')

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        del elems


def process(beg, end, args, outpath):
    try:
        fast_iter(beg, end, args.input_path, outpath)
    except Exception as e:
        logger.error('unexpected error')
        logger.exception(e)


def load_index(input_path, pages_per_chunk=100):
    beg, cur = 0, 0
    page_count = 0
    chunks = []
    for line in bz2.BZ2File(input_path):
        cur += len(line)
        if b'<' not in line:
            continue
        m = tag_patterns.search(line)
        if not m:
            continue
        tag = m.group(2)
        if tag == b'/siteinfo':
            beg = cur
        if tag == b'/page':
            page_count += 1
            if page_count % pages_per_chunk == 0:
                chunks.append((beg, cur))
                beg = cur
    return chunks




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_path',
                        help='path to pages-meta-history\d.xml-\w+.bz2')
    parser.add_argument('outdir', help='output directory')
    parser.add_argument('--nworker', '-n', default=1,
                        help='number of processors to use (default=1)')
    parser.add_argument('--verbose', '-v', default=False, action='store_true',
                        help='verbose logging')
    # parser.add_argument('--index_range', '-i', default=None,
    #                     help='Index range for debug')
    args = parser.parse_args()

    filename = os.path.split(args.input_path)[1].replace('.bz2', '')
    os.makedirs(f'{args.outdir}/chunks', exist_ok=True)

    logger.info('loading index: %s' % args.input_path)
    bz2f_index = load_index(args.input_path)
    logger.info('# of chunks: %s' % len(bz2f_index))

    logger.info('processing...')
    pool = multiprocessing.Pool(processes=int(args.nworker))
    logger.info('# of workers: %s' % args.nworker)
    logger.info('parent pid: %s' % os.getpid())
    for i, j in bz2f_index:
        outpath = f'{args.outdir}/chunks/b_{i}-{j}'
        pool.apply_async(process, args=(i, j, args, outpath,),)
    pool.close()
    pool.join()

    logger.info('done.')
