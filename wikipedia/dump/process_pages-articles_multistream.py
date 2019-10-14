import sys
import os
import io
import re
import argparse
import logging
import multiprocessing
import bz2
import subprocess

from lxml import etree
from xml.etree.cElementTree import iterparse
import ujson as json
from unidecode import unidecode

from utils import wikicorpus


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def fast_iter(beg, end, p_xml, outpath):
    try:
        # Read bz2
        bz2f = io.open(p_xml, 'rb')
        bz2f.seek(beg)
        if end == -1:
            blocks = bz2f.read(-1)
        else:
            blocks = bz2f.read(end-beg)

        pages = '<pages>\n%s</pages>\n' % bz2.decompress(blocks).decode('utf-8')
        if end == -1:
            pages = pages.replace('</mediawiki>', '')
        pages = pages.encode('utf-8') # Convert to bytes

        fw = open(outpath+'.full.tmp', 'w')
        fw_light = open(outpath+'.light.tmp', 'w')
        elems = etree.iterparse(io.BytesIO(pages), events=('end',), tag='page')
        for _, elem in elems:
            ns = elem.find('ns').text
            if ns != '0': # Main page only
                continue

            text = elem.find('revision').find('text').text
            if text == None:
                text = ''

            res = {
                'id': elem.find('id').text,
                'title': elem.find('title').text
            }
            # Redirect
            if elem.find('redirect') is not None:
                res['redirect'] = elem.find('redirect').attrib['title']
                res['redirect'] = res['redirect']
            else:
                res['redirect'] = None

            # Disambiguation
            if re.search('{{disambiguation.*?}}', text.lower()):
                res['disambiguation'] = True
            else:
                res['disambiguation'] = False

            # Article
            if not res['disambiguation']:
                text_with_links = wikicorpus.remove_markup(text, res['title'])
            else:
                text_with_links = wikicorpus.remove_markup(text)
            plain_text, links = wikicorpus.extract_links(text_with_links)

            # Sections
            res['sections'] = wikicorpus.extract_sects(plain_text)

            # Categories
            res['categories'] = wikicorpus.extract_cats(text)

            # Light dumps for merging
            # For more details, check function: merge_output(outdir)
            fw_light.write('%s\n' % json.dumps(res, sort_keys=True))

            # Full dumps
            res['article'] = plain_text
            res['links'] = links
            fw.write('%s\n' % json.dumps(res, sort_keys=True))

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        del elems
        fw.close()

    except Exception:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        msg = 'unexpected error: %s | %s | %s' % \
              (exc_type, exc_obj, exc_tb.tb_lineno)
        logger.error(msg)
        logger.error('%s %s' % (beg, end))


def load_index(pdata):
    res = set()
    for line in bz2.BZ2File(pdata):
        m = re.search((b'(\d+)\:\d+:.+'), line)
        res.add(int(m.group(1)))
    res = list(sorted(res, key=int))[0::100]
    res.append(-1)
    return res


def merge_output(outdir, verbose=True):
    _redirect = {}
    title2id = {}
    disambiguation = set()
    section = {}
    category = {}
    for i in os.listdir('%s/blocks' % outdir):
        if not i.endswith('.light.tmp'):
            continue
        with open('%s/blocks/%s' % (outdir, i), 'r') as f:
            for line in f:
                d = json.loads(line)
                try:
                    assert d['title'] not in title2id
                except AssertionError:
                    if verbose:
                        msg = 'duplicated title: %s | %s | %s' % \
                            (d['title'], d['id'], title2id[d['title']])
                        logger.warning(msg)
                    continue
                title2id[d['title']] = d['id']
                section[d['title']] = d['sections']
                category[d['title']] = d['categories']
                if d['redirect'] is not None:
                    _redirect[d['title']] = d['redirect']
                if d['disambiguation']:
                    disambiguation.add(d['title'])

    redirect = {}
    for title in _redirect:
        re_title = _redirect[title]
        if re_title in title2id:
            redirect[title] = re_title
    msg = '# of wicked redirect links: %s' % (len(_redirect)-len(redirect))
    logger.info(msg)

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

    article = {} # No redirect/disambiguation pages
    for title in title2id:
        if title in redirect:
            continue
        if title in disambiguation:
            continue
        article[title2id[title]] = title

    json.dump(redirect,
              open('%s/redirects.json' % outdir, 'w'), indent=4)
    json.dump(title2id,
              open('%s/title2id.json' % outdir, 'w'), indent=4)
    json.dump(id2title,
              open('%s/id2title_with_redirect.json' % outdir, 'w'), indent=4)
    json.dump(sorted(disambiguation),
              open('%s/disambiguations.json' % outdir, 'w'), indent=4)
    json.dump(article,
              open('%s/articles.json' % outdir, 'w'), indent=4)
    json.dump(section,
              open('%s/sections.json' % outdir, 'w'), indent=4)
    json.dump(category,
              open('%s/categories.json' % outdir, 'w'), indent=4)
    return redirect, title2id, disambiguation


def redirect_links(pdata, outpath, redirect, title2id):
    fw = open(outpath, 'w')
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
            fw.write('%s\n' % json.dumps(d, sort_keys=True))
    fw.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('p_xml',
                        help='Path to pages-articles-multistream.xml.bz')
    parser.add_argument('p_index',
                        help='Path to pages-articles-multistream-index.txt.bz2')
    parser.add_argument('outdir',
                        help='Output directory')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    parser.add_argument('--verbose', '-v', default=False, action='store_true',
                        help='Verbose logger')
    args = parser.parse_args()

    filename = os.path.split(args.p_xml)[1].replace('.xml.bz2', '')
    lang = re.search('(\w+)wiki\-', filename).group(1).replace('_', '-')
    os.makedirs('%s/blocks' % args.outdir, exist_ok=True)

    logger.info('loading index: %s' % args.p_index)
    bz2f_index = load_index(args.p_index)
    logger.info('# of blocks: %s' % len(bz2f_index))
    logger.info('processing...')
    pool = multiprocessing.Pool(processes=int(args.nworker))
    logger.info('# of workers: %s' % args.nworker)
    logger.info('parent pid: %s' % os.getpid())
    for i, j in zip(bz2f_index, bz2f_index[1:]):
        outpath = '%s/blocks/b_%s-%s' % (args.outdir, i, j)
        pool.apply_async(fast_iter, args=(i, j, args.p_xml, outpath,),)
    pool.close()
    pool.join()

    logger.info('merging...')
    redirect, title2id, _ = merge_output(args.outdir, verbose=args.verbose)

    logger.info('revising redirect links...') # TO-DO: multi-processing
    for i in os.listdir('%s/blocks' % args.outdir):
        if not i.endswith('.full.tmp'):
            continue
        inpath ='%s/blocks/%s' % (args.outdir, i)
        outpath = '%s/blocks/%s' % (args.outdir, i.split('.')[0])
        redirect_links(inpath, outpath, redirect, title2id)

    # logger.info('splitting...')
    # cmds = [
    #     'rm %s/blocks/*.tmp' % outdir,
    #     'cat %s/blocks/* > %s/blocks/merged' % (outdir, outdir),
    #     'split -C 20m --numeric-suffixes %s/blocks/merged %s/blocks/' % \
    #     (outdir, outdir),
    #     'rm %s/blocks/merged' % outdir,
    #     'rm %s/blocks/b_*' % outdir,
    # ]
    # for cmd in cmds:
    #     subprocess.call(cmd, shell=True)

    logger.info('cleaning...')
    cmds = [
        'rm %s/blocks/*.tmp' % args.outdir,
    ]
    for cmd in cmds:
        subprocess.call(cmd, shell=True)
    logger.info('done.')
