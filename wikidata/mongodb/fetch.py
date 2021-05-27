import logging
import functools

from pymongo import MongoClient


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


class Wikidata(object):
    def __init__(self, host, port, db_name, collection_name,
                 username=None, password=None):
        if username and password:
            self.client = MongoClient(host=host, port=port,
                                      username=username, password=password)
        else:
            self.client = MongoClient(host=host, port=port)
        self.collection = self.client[db_name][collection_name]

    def get_p31s(self, qid):
        """
        """

        query = {'id': qid}
        project = {
            '_id': 0,
            'id': 1,
            'claims.P31.mainsnak.datavalue.value.id': 1
        }
        res = []
        r = self.collection.find_one(query, project)
        if r:
            try:
                for i in r['claims']['P31']:
                    p31 = i['mainsnak']['datavalue']['value']['id']
                    res.append(p31)
            except KeyError:
                pass
        return res

    def get_label(self, qid, lang='en'):
        """
        """

        query = {'id': qid}
        project = {
            '_id': 0,
            f'labels.{lang}.value': 1,
            'id': 1,
        }
        res = None
        r = self.collection.find_one(query, project)
        try:
            return r['labels'][lang]['value']
        except (KeyError, TypeError):
            return None

    def get_wiki(self, qid, wikisite='enwiki'):
        """
        """

        query = {'id': qid}
        project = {
            '_id': 0,
            f'sitelinks.{wikisite}.title': 1,
            'id': 1
        }
        r = self.collection.find_one(query, project)
        try:
            return r['sitelinks'][wikisite]['title']
        except (KeyError, TypeError):
            return None

    def get_description(self, qid, lang='en'):
        """
        """

        query = {'id': qid}
        project = {
            '_id': 0,
            f'descriptions.{lang}.value': 1,
            'id': 1,
        }
        try:
            r = collection.find_one(query, project)
            return r['descriptions'][lang]['value']
        except (KeyError, TypeError):
            return None

    def get_qids_p31(self, p31):
        """
        """

        query = {'claims.P31.mainsnak.datavalue.value.id': p31}
        project = {
            '_id': 0,
            'claims.P31.mainsnak.datavalue.value.id': 1,
            'id': 1
        }
        res = []
        for i in self.collection.find(query, project):
            res.append(i['id'])
        return res

    def get_qid_wiki(self, wiki, wikisite='enwiki'):
        """
        """

        wiki = wiki.replace('_', ' ')
        query = {f'sitelinks.{wikisite}.title': wiki}
        project = {
            '_id': 0,
            f'sitelinks.{wikisite}.title': 1,
            'id': 1
        }
        # TO-DO: may have 1 to n mapping, return list?
        r = self.collection.find_one(query, project)
        try:
            return r['id']
        except (KeyError, TypeError):
            return None


    def get_all_properties(self):
        """
        """

        pid2name = {}
        query = {'_id': {'$regex': '^P\d+'}}
        for i in self.collection.find(query):
            try:
                pid2name[i['_id']] = i['labels']['en']['value']
            except KeyError:
                msg = f'Cannot find the English label name for {i["_id"]}'
                logger.error(msg)
        return pid2name
