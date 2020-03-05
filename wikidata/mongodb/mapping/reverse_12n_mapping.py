import sys
import os
import logging
from collections import defaultdict
import argparse

import ujson as json
from tqdm import tqdm


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_path', help='Input Path')
    parser.add_argument('output_path', help='Output Path')
    args = parser.parse_args()

    logger.info('loading...')
    with open(args.input_path, 'r') as f:
        data = json.load(f)

    logger.info('reversing...')
    res = defaultdict(list)
    for i in tqdm(data, total=len(data)):
        for j in data[i]:
            res[j].append(i)

    logger.info('writing...')
    with open(args.output_path, 'w') as fw:
        json.dump(res, fw, indent=4)
