import os
import re
import requests
import subprocess
import argparse
import multiprocessing
import urllib.request
import json


def process(cmd):
    cmd += f' -nv -a wget_log_{os.getpid()}'
    print(f'- {os.getpid()}: {cmd}')
    r = subprocess.call(cmd, shell=True)
    while r != 0:
        print(f'- {os.getpid()} retry: {cmd}')
        r = subprocess.call(cmd, shell=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-o', '--output_dir', type=str, default=None, required=True, help='')
    parser.add_argument('-l', '--label', type=str, default=None, required=True, help='')
    parser.add_argument('-n', '--nproc', type=int, default=2, help='')
    args = parser.parse_args()

    url = 'https://commons.wikimedia.org/w/api.php?action=sitematrix&smtype=language&format=json'
    request = urllib.request.Request(url)
    result = urllib.request.urlopen(request).read()
    data = json.loads(result)
    cmds = []
    for i in data['sitematrix']:
        if i == 'count':
            print('number of sites: %d' % data['sitematrix']['count'])
            continue
        d = data['sitematrix'][i]
        lang = d['code']
        for site in d['site']:
            dbname = site['dbname']
            print(lang, dbname)
            url = f'https://dumps.wikimedia.org/{dbname}/latest/'
            r = requests.get(url)
            output_dir = f'{args.output_dir}/{args.label}/{dbname}-{args.label}/meta-history'
            os.makedirs(output_dir, exist_ok=True)
            for line in r.text.split('\n'):
                if re.search('-pages-meta-history', line):
                    m = re.search('<a href="(.*?)">', line)
                    file_name = m.group(1)
                    if '.7z' in file_name:
                        continue
                    if file_name.endswith('-rss.xml'):
                        continue
                    if os.path.exists(f'{args.output_dir}/{file_name}'):
                        continue
                    cmd = f'wget {url}{file_name} -O {output_dir}/{file_name}'
                    cmds.append(cmd)

    with open(f'cmds_{os.getpid()}', "w") as fw:
        fw.write('\n'.join(cmds))

    pool = multiprocessing.Pool(processes=args.nproc)
    for i in cmds:
        pool.apply_async(process, args=(i,),)
    pool.close()
    pool.join()
