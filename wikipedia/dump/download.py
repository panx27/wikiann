import sys
import os
import subprocess
import urllib.request

import ujson as json


def download(wikisite, label, outdir):
    outdir = '%s/%s/%s-%s' % (outdir, label, wikisite, label)
    os.makedirs(outdir, exist_ok=True)
    url = 'https://dumps.wikimedia.org/%s/latest/' % wikisite
    downloads = [
        '-latest-pages-articles-multistream-index.txt.bz2',
        '-latest-pages-articles-multistream.xml.bz2',
        '-latest-page.sql.gz',
        '-latest-langlinks.sql.gz',
        '-latest-site_stats.sql.gz',
        '-latest-image.sql.gz',
        '-latest-imagelinks.sql.gz',
        '-latest-externallinks.sql.gz',
        '-latest-category.sql.gz',
        '-latest-categorylinks.sql.gz'
    ]
    cmds = []
    for i in downloads:
        cmds.append('wget %s%s%s -O %s/%s%s' %
                    (url, wikisite, i,
                     outdir, wikisite, i.replace('latest', label)))
    for cmd in cmds:
        cmd += f' -nv -a wget_log_{os.getpid()}'
        subprocess.call(cmd, shell=True)


def download_all(label, outdir):
    """SITE API:
    https://commons.wikimedia.org/w/api.php?action=sitematrix&smtype=language&format=json
       STATS API:
    https://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=statistics&format=json
    """
    url = 'https://commons.wikimedia.org/w/api.php?action=sitematrix&smtype=language&format=json'
    request = urllib.request.Request(url)
    result = urllib.request.urlopen(request).read()
    data = json.loads(result)
    n = 0
    for i in data['sitematrix']:
        if i == 'count':
            print('number of sites: %d' % data['sitematrix']['count'])
            continue
        d = data['sitematrix'][i]
        lang = d['code']
        for site in d['site']:
            dbname = site['dbname']
            print(lang, dbname)
            n += 1
            download(dbname, label, outdir)
    print(n)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('USAGE: <wikisite (e.g., enwiki)> <label (e.g., 20180401)> '
              '<output dir>')
        exit()
    wikisite = sys.argv[1]
    label = sys.argv[2]
    outdir = sys.argv[3]
    if wikisite == 'all':
        download_all(label, outdir)
    else:
        download(wikisite, label, outdir)
