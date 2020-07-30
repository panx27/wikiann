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


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('path_p279', help='Path to p279 mapping')
#     parser.add_argument('path_p31', help='Path to p31 mapping')
#     parser.add_argument('path_labels', help='Path to labels.lang.value mapping')
#     parser.add_argument('output_dir', help='Output Directory')
#     args = parser.parse_args()

#     with open(args.path_p279, 'r') as f:
#         qid2p279 = json.load(f)
#     with open(args.path_p31, 'r') as f:
#         qid2p31 = json.load(f)
#     with open(args.path_labels, 'r') as f:
#         qid2labels = json.load(f)

#     _taxonomy_p2c = defaultdict(list) # Parent to children
#     for i in tqdm(qid2p279, total=len(qid2p279)):
#         if i not in qid2labels: # Throw away the ones without label names
#             continue
#         for j in qid2p279[i]:
#             if j not in qid2labels: # Throw away the ones without label names
#                 continue
#             _taxonomy_p2c[j].append(i) # Example: i = racing car, j = car
#             _taxonomy_p2c[i] # Create an empty list

#     to_remove = set() # Leaf node should not be an instance (P31)
#     for i in _taxonomy_p2c:
#         if len(_taxonomy_p2c[i]) == 0 and i in qid2p31:
#             to_remove.add(i)
#     taxonomy_p2c = {}
#     for i in _taxonomy_p2c:
#         if i in to_remove:
#             continue
#         nodes = []
#         for j in _taxonomy_p2c[i]:
#             if j in to_remove:
#                 continue
#             nodes.append(j)
#         taxonomy_p2c[i] = nodes

#     labels = {}
#     for i in taxonomy_p2c:
#         labels[i] = qid2labels[i]
#         for j in taxonomy_p2c[i]:
#             labels[j] = qid2labels[j]

#     # with open(f'{args.output_dir}/taxonomy_c2p.json', 'w') as fw:
#     #     json.dump(taxonomy_c2p, fw, indent=4)
#     with open(f'{args.output_dir}/taxonomy_p2c.json', 'w') as fw:
#         json.dump(taxonomy_p2c, fw, indent=4)
#     with open(f'{args.output_dir}/taxonomy_labels.json', 'w') as fw:
#         json.dump(labels, fw, indent=4)













if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('path_p279', help='Path to p279 mapping')
    parser.add_argument('path_p31', help='Path to p31 mapping')
    parser.add_argument('path_labels', help='Path to labels.lang.value mapping')
    parser.add_argument('output_dir', help='Output Directory')
    args = parser.parse_args()

    with open(args.path_p279, 'r') as f:
        qid2p279 = json.load(f)
    with open(args.path_p31, 'r') as f:
        qid2p31 = json.load(f)
    with open(args.path_labels, 'r') as f:
        qid2labels = json.load(f)

    _taxonomy_p2c = defaultdict(set) # Parent to children
    for i in tqdm(qid2p279, total=len(qid2p279)):
        if i not in qid2labels: # Throw away the ones without label names
            continue
        for j in qid2p279[i]:
            if j not in qid2labels: # Throw away the ones without label names
                continue

            if i in qid2p31:
                a = qid2p31[i]
            else:
                a = [i]
            if j in qid2p31:
                b = qid2p31[j]
            else:
                b = [j]

            for x in a:
                if x not in qid2labels:
                    continue
                for y in b:
                    if y not in qid2labels:
                        continue
                    _taxonomy_p2c[y].add(x) # Example: i = racing car, j = car
                    _taxonomy_p2c[x] # Create an empty list

    # to_remove = set() # Leaf node should not be an instance (P31)
    # for i in _taxonomy_p2c:
    #     if len(_taxonomy_p2c[i]) == 0 and i in qid2p31:
    #         to_remove.add(i)
    # taxonomy_p2c = {}
    # for i in _taxonomy_p2c:
    #     if i in to_remove:
    #         continue
    #     nodes = []
    #     for j in _taxonomy_p2c[i]:
    #         if j in to_remove:
    #             continue
    #         nodes.append(j)
    #     taxonomy_p2c[i] = nodes
    taxonomy_p2c = _taxonomy_p2c

    labels = {}
    for i in taxonomy_p2c:
        labels[i] = qid2labels[i]
        for j in taxonomy_p2c[i]:
            labels[j] = qid2labels[j]

    # with open(f'{args.output_dir}/taxonomy_c2p.json', 'w') as fw:
    #     json.dump(taxonomy_c2p, fw, indent=4)
    with open(f'{args.output_dir}/taxonomy_p2c.json', 'w') as fw:
        json.dump(taxonomy_p2c, fw, indent=4)
    with open(f'{args.output_dir}/taxonomy_labels.json', 'w') as fw:
        json.dump(labels, fw, indent=4)
