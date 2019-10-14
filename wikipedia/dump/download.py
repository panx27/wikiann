import sys
import os
import subprocess
import urllib.request
import ujson as json


def download(wikisite, date, outdir):
    outdir = '%s/%s/%s-%s' % (outdir, date, wikisite, date)
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
    ]
    cmds = []
    for i in downloads:
        cmds.append('wget %s%s%s -O %s/%s%s' % \
                    (url, wikisite, i,
                     outdir, wikisite, i.replace('latest', date)))
    for i in cmds:
        subprocess.call(i, shell=True)


def download_all(date, outdir):
    '''
    SITE API:
    https://commons.wikimedia.org/w/api.php?action=sitematrix&smtype=language&format=json
    STATS API:
    https://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=statistics&format=json
    '''

    url = 'https://commons.wikimedia.org/w/api.php?action=sitematrix&smtype=language&format=json'
    request = urllib.request.Request(url)
    result = urllib.request.urlopen(request).read()
    data = json.loads(result)
    for i in data['sitematrix']:
        if i == 'count':
            continue
        d = data['sitematrix'][i]
        lang = d['code']
        try:
            dbname = d['site'][0]['dbname']
        except IndexError:
            continue
        print(lang, dbname)
        download(dbname, date, outdir)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('USAGE: <wikisite (e.g., enwiki)> <date (e.g., 20180401)> <output dir>')
        exit()
    wikisite = sys.argv[1]
    date = sys.argv[2]
    outdir = sys.argv[3]
    if wikisite == 'all':
        download_all(date, outdir)
    else:
        download(wikisite, date, outdir)
