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
    parser.add_argument('--username', '-u', default=None,
                        help='Username (if authentication is enabled)')
    parser.add_argument('--password', '-p', default=None,
                        help='Password (if authentication is enabled)')
    args = parser.parse_args()

    host = args.host
    port = int(args.port)
    db_name = args.db_name
    collection_name = args.collection_name
    output_path = args.output_path
    username = args.username
    password = args.password

    logger.info(f'db name: {db_name}')
    logger.info(f'collection name: {collection_name}')
    if username and password:
        client = MongoClient(host=host, port=port,
                             username=username, password=password)
    else:
        client = MongoClient(host=host, port=port)

    collection = client[db_name][collection_name]

    # Do not use {'$exists': True}
    query = {'sitelinks.zhwiki.title': {'$regex': '.+'}}
    project = {'sitelinks.zhwiki.title': 1, 'id': 1, '_id': 0}
    res = collection.find(query, project)
    # stats = res.explain()["executionStats"]
    # pprint.pprint(stats)
    # total = stats['nReturned']
    total = collection.count_documents(query)
    qid2enwiki = {}
    for i in tqdm(res, total=total):
        qid2enwiki[i['id']] = i['sitelinks']['zhwiki']['title']

    with open(output_path, 'w') as fw:
        json.dump(qid2enwiki, fw, indent=4)
