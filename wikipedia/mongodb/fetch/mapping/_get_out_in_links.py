import sys
import os
import logging
from itertools import islice
import multiprocessing
import pprint
from collections import defaultdict
import argparse

import ujson as json
from pymongo import MongoClient
from tqdm import tqdm


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('host', help='MongoDB host')
    parser.add_argument('port', help='MongoDB port')
    parser.add_argument('db_name', help='Database name')
    parser.add_argument('collection_name', help='Collection name')
    parser.add_argument('output_dir', help='Output Directory')
    args = parser.parse_args()

    host = args.host
    port = int(args.port)
    db_name = args.db_name
    collection_name = args.collection_name
    output_dir = args.output_dir

    logger.info(f'db name: {db_name}')
    logger.info(f'collection name: {collection_name}')
    client = MongoClient(host=host, port=port)
    collection = client[db_name][collection_name]

    query = {'title': {'$regex': '.+'}}
    project = {
        'title': 1,
        '_id': 0,
        'links.title': 1
    }
    res = collection.find(query, project)
    # stats = res.explain()["executionStats"]
    # pprint.pprint(stats)
    # total = stats['nReturned']
    total = collection.count_documents(query)
    outlinks = defaultdict(set)
    inlinks = defaultdict(set)
    for i in tqdm(res, total=total):
        for j in i['links']:
            outlinks[i['title']].add(j['title'])
            inlinks[j['title']].add(i['title'])

    with open(f'{output_dir}/outlinks.json', 'w') as fw:
        json.dump(outlinks, fw, indent=4)
    with open(f'{output_dir}/inlinks.json', 'w') as fw:
        json.dump(inlinks, fw, indent=4)
