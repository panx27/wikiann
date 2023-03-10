import os
import re
import requests
import subprocess
import argparse
import multiprocessing


def process(cmd):
    cmd += f' -nv -a wget_log_{os.getpid()}'
    print(cmd)
    subprocess.call(cmd, shell=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-o', '--output_dir', type=str, default=None, required=True, help='')
    parser.add_argument('-n', '--nproc', type=int, default=2, help='')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    url = 'https://dumps.wikimedia.org/enwiki/latest/'
    r = requests.get(url)
    cmds = []
    for line in r.text.split('\n'):
        if re.search('-latest-pages-meta-history', line):
            m = re.search('<a href="(.*?)">', line)
            file_name = m.group(1)
            if '.7z' in file_name:
                continue
            if os.path.exists(f'{args.output_dir}/{file_name}'):
                continue
            cmd = f'wget {url}{file_name} -O {args.output_dir}/{file_name}'
            cmds.append(cmd)
            # subprocess.call(cmd, shell=True)

    pool = multiprocessing.Pool(processes=args.nproc)
    for i in cmds:
        pool.apply_async(process, args=(i,),)
    pool.close()
    pool.join()