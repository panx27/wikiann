import sys
import os
import io
import re
import bz2
import json
import argparse
import logging
import multiprocessing
import subprocess
from datetime import datetime

from lxml import etree
from xml.etree.cElementTree import iterparse, dump

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from bson import json_util

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


def fast_iter(beg, end, input_path, args):
    filename = os.path.split(input_path)[1]

    client = MongoClient(host=args.host, port=args.port,
                        username=args.username, password=args.password)
    collection = client[args.db_name][args.collection_name]

    # Using index to seek bz2
    with bz2.BZ2File(input_path) as f:
        if args.verbose:
            logger.info(f"{os.getpid()}: seeking {beg}")
        f.seek(beg)
        if args.verbose:
            logger.info(f"{os.getpid()}: reading {end - beg}")
        if end == -1:
            chunks = f.read(-1)
        else:
            chunks = f.read(end - beg)
        # Wrap up xml bytes
        chunks = b'<pages>' + chunks + b'</pages>'

        if args.verbose:
            logger.info(f"{os.getpid()}: parsing")
        elems = etree.iterparse(io.BytesIO(chunks), events=('end',), tag='page', huge_tree=True)
        for _, elem in elems:
            ns = int(elem.find('ns').text)
            # if ns != '0':  # Main page (ns == 0) only
            #     continue

            page_id = elem.find('id').text
            page_title = elem.find('title').text
            # Redirect
            if elem.find('redirect') is not None:
                redirect = elem.find('redirect').attrib['title']
            else:
                redirect = None

            extractor = Extractor(page_id, 0, '', page_title, [])
            res = []
            for n, i in enumerate(elem.findall('revision')):
                rev = {
                    '_chunk_id': f'{filename}_{beg}:{end}',
                    'ns': ns,
                    'page_id': page_id,
                    'page_title': page_title,
                    'redirect': redirect,
                    'id': i.find('id').text,
                    'ts': i.find('timestamp').text,
                    'idx': n,
                }
                rev['_id'] = f'{page_id}_{rev["id"]}_{rev["idx"]}'
                rev['ts'] = datetime.strptime(rev['ts'], "%Y-%m-%dT%H:%M:%SZ")
                try:
                    rev['parentid'] = i.find('parentid').text
                except AttributeError:
                    rev['parentid'] = None
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
                plain_text, links, exlinks = replace_links('\n'.join(paragraphs))

                rev['text'] = plain_text
                rev['links'] = links
                rev['infobox'] = extract_infobox(raw_markup)
                rev['sections'] = extract_sections(plain_text)
                rev['categories'] = extract_categories(raw_markup)
                rev['external_links'] = exlinks

                res.append(rev)

            try:
                r = collection.insert_many(res)
                # logger.info(f"{os.getpid()}: {str(r)}")
            except UnicodeEncodeError:
                res = json.loads(
                    json.dumps(
                        res,
                        ensure_ascii=False,
                        default=json_util.default
                    ).encode("utf-8", "ignore").decode(),
                    object_hook=json_util.object_hook
                )
                for n, i in enumerate(res):
                    try:
                        collection.insert_one(i)
                    except DuplicateKeyError:
                        pass
                    except UnicodeEncodeError:
                        logger.error(f"Still get UnicodeEncodeError: {i['_chunk_id']} {i['_id']}")

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        del elems
    client.close()
    if args.verbose:
        logger.info(f"{os.getpid()}: done.")


def process(beg, end, args):
    try:
        fast_iter(beg, end, args.input_path, args)
    except Exception as e:
        logger.error('unexpected error')
        logger.error(args.input_path, beg, end)
        logger.exception(e)


def load_index(input_path, pages_per_chunk=100):
    beg, cur = 0, 0
    page_count = 0
    chunks = []
    with bz2.BZ2File(input_path) as f:
        for line in f:
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


def merge_index(intervals, n):
    while len(intervals) > n:
        # Find best interval to merge
        best_idx = None
        best_diff = float('inf')

        for i in range(len(intervals) - 1):
            # Check if the end of the current interval matches the beginning of the next
            if intervals[i][1] == intervals[i+1][0]:
                diff = intervals[i+1][1] - intervals[i][0]
                if diff < best_diff:
                    best_diff = diff
                    best_idx = i

        # If no merge candidate was found, break
        if best_idx is None:
            break

        # Merge the intervals
        merged_interval = [intervals[best_idx][0], intervals[best_idx+1][1]]
        intervals[best_idx] = merged_interval
        intervals.pop(best_idx + 1)

    return intervals


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_path', required=True,
                        help='path to pages-meta-history\d.xml-\w+.bz2')
    parser.add_argument('--nworker', '-n', default=1,
                        help='number of processors to use (default=1)')
    parser.add_argument('--verbose', '-v', default=False, action='store_true',
                        help='verbose logging')
    parser.add_argument('--host', required=True, help='MongoDB host')
    parser.add_argument('--port', type=int, required=True, help='MongoDB port')
    parser.add_argument('--db_name', required=True, help='Database name')
    parser.add_argument('--collection_name', required=True, help='Collection name')
    parser.add_argument('--username', '-u', default=None,
                        help='Username (if authentication is enabled)')
    parser.add_argument('--password', '-p', default=None,
                        help='Password (if authentication is enabled)')
    args = parser.parse_args()

    if os.path.exists(f'{args.input_path}.idx'):
        logger.info('loading index: %s.idx' % args.input_path)
        with open(f'{args.input_path}.idx', 'r') as f:
            bz2f_index = json.load(f)
    else:
        logger.info('loading index: %s' % args.input_path)
        bz2f_index = load_index(args.input_path)
        with open(f'{args.input_path}.idx', 'w') as fw:
            json.dump(bz2f_index, fw)
    logger.info('# of chunks: %s' % len(bz2f_index))
    if len(bz2f_index) > int(args.nworker):
        bz2f_index = merge_index(bz2f_index, int(args.nworker))
        logger.info('# of chunks after merging: %s' % len(bz2f_index))

    logger.info('processing...')
    pool = multiprocessing.Pool(processes=int(args.nworker))
    logger.info('# of workers: %s' % args.nworker)
    logger.info('parent pid: %s' % os.getpid())
    for i, j in bz2f_index:
        pool.apply_async(process, args=(i, j, args,),)
    pool.close()
    pool.join()

    logger.info('done.')
