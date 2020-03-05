import logging


logger = logging.getLogger()


def get_all_properties(collection):
    pid2name = {}
    query = {'_id': {'$regex': '^P\d+'}}
    for i in collection.find(query):
        try:
            pid2name[i['_id']] = i['labels']['en']['value']
        except KeyError:
            logger.error('Cannot find the English label name for %s' % i['_id'])

    return pid2name


def get_description(collection, _id, lang='en'):
    try:
        trans = {'descriptions.%s.value' % lang: 1, 'id': 1, '_id': 0}
        r = collection.find_one({'id': _id}, trans)
        return r['descriptions'][lang]['value']
    except (KeyError, TypeError):
        return None


def get_wiki_title(collection, _id, wikisite='enwiki'):
    try:
        trans = {'sitelinks.%s.title' % wikisite: 1, 'id': 1, '_id': 0}
        r = collection.find_one({'id': _id}, trans)
        return r['sitelinks']['enwiki']['title']
    except (KeyError, TypeError):
        return None


def get_label_name(collection, _id, lang='en'):
    try:
        trans = {'labels.%s.value' % lang: 1, 'id': 1, '_id': 0}
        r = collection.find_one({'id': _id}, trans)
        return r['labels'][lang]['value']
    except (KeyError, TypeError):
        return None


def get_all_p31(collection, _id):
    p31 = []
    r = collection.find_one({'id': _id})
    for i in r['claims']['P31']:
        p31.append(j['mainsnak']['datavalue']['value']['id'])
    return p31
