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
    parser.add_argument('output_path', help='Output Path')
    args = parser.parse_args()

    host = args.host
    port = int(args.port)
    db_name = args.db_name
    collection_name = args.collection_name
    output_path = args.output_path

    logger.info(f'db name: {db_name}')
    logger.info(f'collection name: {collection_name}')
    client = MongoClient(host=host, port=port)
    collection = client[db_name][collection_name]

    query = {'properties': {'$in': ['P279']}}
    trans = {
        'id': 1,
        '_id': 0,
        'claims.P279.mainsnak.datavalue.value.id': 1,
    }
    res = collection.find(query, trans)
    # stats = res.explain()["executionStats"]
    # pprint.pprint(stats)
    # total = stats['nReturned']
    total = collection.count_documents(query)

    qid2p279 = defaultdict(list)
    for i in tqdm(res, total=total):
        for j in i['claims']['P279']:
            try:
                p279 = j['mainsnak']['datavalue']['value']['id']
                qid2p279[i['id']].append(p279)
            except KeyError:
                pass

    with open(output_path, 'w') as fw:
        json.dump(qid2p279, fw, indent=4)