import os
import re
import subprocess


PARSE_PAGES_ARTICLES_MULTISTREAM = '/data/home/xiaomanpan/code/wikiann/wikipedia/dump/parse_pages_articles_multistream.py'
ANNOTATE_PAGES_ARTICLES = '/data/home/xiaomanpan/code/wikiann/wikipedia/dump/annotate.py'

indir = '/cephfs/user/xiaomanpan/data/wikipedia/20230120'
langs = []


for i in os.listdir(indir):
    if not re.search('(\w+)wiki\-', i):
        continue
    lang = re.search('(\w+)wiki\-', i).group(1).replace('_', '-')
    if langs and lang not in langs:
        continue

    # parse_pages_articles_multistream.py
    if os.path.isdir('%s/%s' % (indir, i)) and \
       not 'output' in os.listdir('%s/%s' % (indir, i)):
        print(i)
        cmd = [
            'python',
            PARSE_PAGES_ARTICLES_MULTISTREAM,
            '%s/%s/%s-pages-articles-multistream.xml.bz2' % (indir, i, i),
            '%s/%s/%s-pages-articles-multistream-index.txt.bz2' % (indir, i, i),
            '%s/%s/output' % (indir, i),
            '-n 48'
        ]
        print(cmd)
        subprocess.call(' '.join(cmd), shell=True)

    # # annotate_pages_articles.py
    # if os.path.isdir('%s/%s' % (indir, i)) and \
    #    not 'blocks.ann' in os.listdir('%s/%s/output' % (indir, i)):
    #     print(i)
    #     cmd = [
    #         'python',
    #         ANNOTATE_PAGES_ARTICLES,
    #         '%s/%s/output/blocks/' % (indir, i),
    #         '%s/%s/output/blocks.ann/' % (indir, i),
    #         lang,
    #         '-n 24'
    #     ]
    #     print(cmd)
    #     subprocess.call(' '.join(cmd), shell=True)

    # # import_wiki_sentence
    # host = '0.0.0.0'
    # port = '12180'
    # planglink = '%s/%s/langlinks/split/%s.json' % (indir, enwiki, lang)
    # if os.path.isdir('%s/%s' % (indir, i)):
    #     print(i)
    #     cmd = [
    #         'python',
    #         '/nas/data/m1/panx2/code/wikipedia-dump-processor/mongodb/import/import_sentences.py',
    #         '%s/%s/output/blocks.pp/' % (indir, i),
    #         host,
    #         port,
    #         'wikipp',
    #         '%s_ll_en' % lang,
    #         '-p %s' % planglink,
    #         '-n 12'
    #     ]
    #     print(cmd)
    #     subprocess.call(' '.join(cmd), shell=True)

    # # delete output
    # if os.path.isdir('%s/%s' % (indir, i)):
    #     print(i)
    #     cmd = 'rm -r %s/%s/wikiann' % (indir, i),
    #     subprocess.call(cmd, shell=True)

    # # delete blocks.ann/
    # if os.path.isdir('%s/%s/wikiann' % (indir, i)):
    #     print(i)
    #     cmd = 'rm -r %s/%s/wikiann/blocks.ann' % (indir, i),
    #     subprocess.call(cmd, shell=True)
