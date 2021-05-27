import logging
import argparse

from pymongo import MongoClient


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('host', help='MongoDB host')
    parser.add_argument('port', help='MongoDB port')
    parser.add_argument('db_name', help='Database name')
    parser.add_argument('collection_name', help='Collection name')
    parser.add_argument('--username', '-u', default=None,
                        help='Username (if authentication is enabled)')
    parser.add_argument('--password', '-p', default=None,
                        help='Password (if authentication is enabled)')
    args = parser.parse_args()

    host = args.host
    port = int(args.port)
    db_name = args.db_name
    collection_name = args.collection_name
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

    logger.info('indexing...')
    # { id: 1 }
    logger.info('index key: { id: 1 }')
    collection.create_index('id', unique=True)

    # { aliases.en.value: 1 }
    logger.info('index key: { aliases.en.value: 1 }')
    key = [('aliases.en.value', 1)]
    pfe = {'aliases.en.value': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { sitelinks.enwiki.title: 1 }
    logger.info('index key: { sitelinks.enwiki.title : 1 }')
    key = [('sitelinks.enwiki.title', 1)]
    pfe = {'sitelinks.enwiki.title': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { sitelinks.enwiki.title: 1, id: 1 }
    logger.info('index key: { sitelinks.enwiki.title: 1, id: 1 }')
    key = [('sitelinks.enwiki.title', 1), ('id', 1)]
    pfe = {'sitelinks.enwiki.title': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { claims.P646.mainsnak.datavalue.value: 1 }
    logger.info('index key: { claims.P646.mainsnak.datavalue.value: 1 }')
    key = [('claims.P646.mainsnak.datavalue.value', 1)]
    pfe = {'claims.P646.mainsnak.datavalue.value': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { labels.en.value: 1, id: 1 }
    logger.info('index key: { labels.en.value: 1, id: 1 }')
    key = [('labels.en.value', 1), ('id', 1)]
    pfe = {'labels.en.value': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { claims.P279.mainsnak.datavalue.value.id: 1 }
    logger.info('index key: { claims.P279.mainsnak.datavalue.value.id: 1 }')
    key = [('claims.P279.mainsnak.datavalue.value.id', 1)]
    pfe = {'claims.P279.mainsnak.datavalue.value.id': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { claims.P31.mainsnak.datavalue.value.id: 1 }
    logger.info('index key: { claims.P31.mainsnak.datavalue.value.id: 1 }')
    key = [('claims.P31.mainsnak.datavalue.value.id', 1)]
    pfe = {'claims.P31.mainsnak.datavalue.value.id': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { properties: 1 }
    logger.info('index key: { properties: 1 }')
    key = [('properties', 1)]
    pfe = {'properties': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { sitelinks.zhwiki.title: 1, id: 1 }
    logger.info('index key: { sitelinks.zhwiki.title: 1, id: 1 }')
    key = [('sitelinks.zhwiki.title', 1), ('id', 1)]
    pfe = {'sitelinks.zhwiki.title': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)
