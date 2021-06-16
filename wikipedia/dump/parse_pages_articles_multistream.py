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
from wikiextractor.wikiextractor.extract import Extractor
from common.utils import replace_links
from common.utils import extract_sections, extract_categories, extract_infobox



logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)

disambiguation_page_patterns = [
    re.compile(r'({{disambiguation.*?}})', re.I),
    re.compile(r'({{.*?disambiguation}})', re.I),
    re.compile(r'({{hndis.*?}})', re.I)
]


def fast_iter(beg, end, p_xml, outpath):
    # Using index to seek bz2
    bz2f = io.open(p_xml, 'rb')
    bz2f.seek(beg)
    if end == -1:
        blocks = bz2f.read(-1)
    else:
        blocks = bz2f.read(end - beg)

    # Convert bytes to str, and wrap up XML string
    decompressed_blocks = bz2.decompress(blocks).decode('utf-8')
    pages = f'<pages>\n{decompressed_blocks}</pages>\n'
    if end == -1:
        pages = pages.replace('</mediawiki>', '')
    pages = pages.encode('utf-8')  # Convert str back to bytes

    with open(f'{outpath}.full.tmp', 'w') as fw, \
         open(f'{outpath}.light.tmp', 'w') as fw_light:
        elems = etree.iterparse(io.BytesIO(pages), events=('end',), tag='page')
        for _, elem in elems:
            ns = elem.find('ns').text
            if ns != '0':  # Main page (ns == 0) only
                continue

            res = {
                'id': elem.find('id').text,
                'title': elem.find('title').text
            }

            raw_markup = elem.find('revision').find('text').text
            if raw_markup is None:
                raw_markup = ''

            # Redirect
            if elem.find('redirect') is not None:
                res['redirect'] = elem.find('redirect').attrib['title']
            else:
                res['redirect'] = None

            # Disambiguation
            # https://en.wikipedia.org/wiki/Template:Disambiguation
            res['disambiguation'] = False
            for dp in disambiguation_page_patterns:
                if re.search(dp, raw_markup) and not res['redirect']:
                    res['disambiguation'] = True
                    break

            # Light JSON dump only contains:
            # `id`, `title`, `redirect`, `disambiguation`.
            # It is used for redirection, see function: merge_output()
            fw_light.write(f'{json.dumps(res)}\n')

            # Article
            # text_with_links = wikimarkup.remove_markup(raw_markup)
            # plain_text, links = wikimarkup.extract_links(text_with_links)

            extractor = Extractor(res['id'], 0, '', res['title'], [])
            paragraphs = extractor.clean_text(raw_markup,
                                              mark_headers=True,
                                              expand_templates=False,
                                              html_safe=False)
            plain_text, links, elinks = replace_links('\n'.join(paragraphs))
            # if len(links) != len(links_) or \
            # set(([x['title'] for x in links])) != set(([x['title'] for x in links_])):
            #     with open('tmp/foo/%s_a' % res['title'], 'w') as fwt:
            #         fwt.write(str([x['title'] for x in links])+'\n\n')
            #         fwt.write('\n'.join([x for x in text_with_links.split('\n') if x])+'\n\n')
            #     with open('tmp/foo/%s_b' % res['title'], 'w') as fwt:
            #         fwt.write(str([x['title'] for x in links_])+'\n\n')
            #         fwt.write('\n'.join(paragraphs)+'\n\n')

            # Sections
            res['sections'] = extract_sections(plain_text)

            # Categories
            res['categories'] = extract_categories(raw_markup)

            # Infobox
            res['infobox'] = extract_infobox(raw_markup)

            # Full dumps
            res['article'] = plain_text
            res['links'] = links
            res['external_links'] = elinks
            fw.write(f'{json.dumps(res)}\n')

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        del elems


def process(beg, end, args, outpath):
    try:
        fast_iter(beg, end, args.p_xml, outpath)
    except Exception as e:
        logger.error('unexpected error')
        logger.exception(e)


def load_index(pdata, index_range=None):
    if index_range:
        res = [int(x) for x in index_range.split(':')]
        res = list(sorted(res, key=int))
    else:
        res = set()
        for line in bz2.BZ2File(pdata):
            m = re.search((b'(\d+)\:\d+:.+'), line)
            res.add(int(m.group(1)))
        # 100 * 100 = 10,000 pages per chunk
        res = list(sorted(res, key=int))[0::100]
        res.append(-1)
    return res


def merge_output(outdir, verbose=True):
    _redirect = {}
    title2id = {}
    disambiguation = set()
    for i in os.listdir(f'{outdir}/blocks'):
        if not i.endswith('.light.tmp'):
            continue
        with open(f'{outdir}/blocks/{i}', 'r') as f:
            for line in f:
                d = json.loads(line)
                try:
                    assert d['title'] not in title2id
                except AssertionError:
                    if verbose:
                        logger.warning(f'duplicated title: {d["title"]} | '
                                       f'{d["id"]} | {title2id[d["title"]]}')
                    continue
                title2id[d['title']] = d['id']
                if d['redirect'] is not None:
                    _redirect[d['title']] = d['redirect']
                if d['disambiguation']:
                    disambiguation.add(d['title'])

    redirect = {}
    for title in _redirect:
        re_title = _redirect[title]
        if re_title in title2id:
            redirect[title] = re_title
    logger.info(f'# of wicked redirect links: {len(_redirect)-len(redirect)}')

    id2title = {}
    for title in title2id:
        if title in redirect:
            _title = redirect[title]
        else:
            _title = title
        id2title[title2id[title]] = {
            'id': title2id[_title],
            'title': _title
        }

    article = {}  # Non redirect and disambiguation pages
    for title in title2id:
        if title in redirect:
            continue
        if title in disambiguation:
            continue
        article[title2id[title]] = title

    with open(f'{outdir}/redirect.json', 'w') as fw:
        json.dump(redirect, fw, indent=4)
    with open(f'{outdir}/title2id.json', 'w') as fw:
        json.dump(title2id, fw, indent=4)
    with open(f'{outdir}/id2title_redirected.json', 'w') as fw:
        json.dump(id2title, fw, indent=4)
    with open(f'{outdir}/disambiguation.json', 'w') as fw:
        json.dump(sorted(disambiguation), fw, indent=4)
    with open(f'{outdir}/article.json', 'w') as fw:
        json.dump(article, fw, indent=4)

    return redirect, title2id, disambiguation


def redirect_links(pdata, outpath, redirect, title2id):
    with open(outpath, 'w') as fw:
        with open(pdata, 'r') as f:
            for line in f:
                d = json.loads(line)
                for link in d['links']:
                    if link['title'] in redirect:
                        link['title'] = redirect[link['title']]
                    if link['title'] in title2id:
                        link['id'] = title2id[link['title']]
                    else:
                        link['id'] = None
                fw.write(f'{json.dumps(d)}\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('p_xml',
                        help='path to pages-articles-multistream.xml.bz')
    parser.add_argument('p_index',
                        help='path to pages-articles-multistream-index.txt.bz2')
    parser.add_argument('outdir',
                        help='output directory')
    parser.add_argument('--nworker', '-n', default=1,
                        help='number of processors to use (default=1)')
    parser.add_argument('--verbose', '-v', default=False, action='store_true',
                        help='verbose logging')
    parser.add_argument('--index_range', '-i', default=None,
                        help='Index range for debug')
    args = parser.parse_args()

    filename = os.path.split(args.p_xml)[1].replace('.xml.bz2', '')
    lang = re.search(r'(\w+)wiki\-', filename).group(1).replace('_', '-')
    os.makedirs(f'{args.outdir}/blocks', exist_ok=True)

    logger.info('loading index: %s' % args.p_index)
    bz2f_index = load_index(args.p_index, args.index_range)
    logger.info('# of blocks: %s' % len(bz2f_index))

    logger.info('processing...')
    pool = multiprocessing.Pool(processes=int(args.nworker))
    logger.info('# of workers: %s' % args.nworker)
    logger.info('parent pid: %s' % os.getpid())
    for i, j in zip(bz2f_index, bz2f_index[1:]):
        outpath = f'{args.outdir}/blocks/b_{i}-{j}'
        pool.apply_async(process, args=(i, j, args, outpath,),)
    pool.close()
    pool.join()

    logger.info('merging...')
    redirect, title2id, _ = merge_output(args.outdir, verbose=args.verbose)

    logger.info('revising redirect links...')  # TO-DO: multi-processing
    for i in os.listdir('%s/blocks' % args.outdir):
        if not i.endswith('.full.tmp'):
            continue
        inpath = f'{args.outdir}/blocks/{i}'
        outpath = f'{args.outdir}/blocks/{i.split(".")[0]}'
        redirect_links(inpath, outpath, redirect, title2id)

    logger.info('cleaning...')
    cmds = [
        f'rm {args.outdir}/blocks/*.tmp',
    ]
    for cmd in cmds:
        subprocess.call(cmd, shell=True)
    logger.info('done.')
