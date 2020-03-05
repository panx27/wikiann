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
    parser.add_argument('path_p279', help='Path to p279 mapping')
    parser.add_argument('path_labels', help='Path to labels.en.value mapping')
    parser.add_argument('output_dir', help='Output Directory')
    args = parser.parse_args()

    with open(args.path_p279, 'r') as f:
        qid2p279 = json.load(f)
    with open(args.path_labels, 'r') as f:
        qid2labels = json.load(f)

    taxonomy = defaultdict(list)
    taxonomy_en = defaultdict(list)
    taxonomy_p2c = defaultdict(list)
    taxonomy_p2c_en = defaultdict(list)
    for i in tqdm(qid2p279, total=len(qid2p279)):
        if i not in qid2labels:
            continue
        for j in qid2p279[i]:
            if j not in qid2labels:
                continue
            taxonomy[i].append(j)
            taxonomy_en[qid2labels[i]].append(qid2labels[j])
            taxonomy_p2c[j].append(i)
            taxonomy_p2c_en[qid2labels[j]].append(qid2labels[i])

    with open(f'{args.output_dir}/taxonomy.json', 'w') as fw:
        json.dump(taxonomy, fw, indent=4)
    with open(f'{args.output_dir}/taxonomy_en_labels.json', 'w') as fw:
        json.dump(taxonomy_en, fw, indent=4)
    with open(f'{args.output_dir}/taxonomy_p2c.json', 'w') as fw:
        json.dump(taxonomy_p2c, fw, indent=4)
    with open(f'{args.output_dir}/taxonomy_p2c_en_labels.json', 'w') as fw:
        json.dump(taxonomy_p2c_en, fw, indent=4)
