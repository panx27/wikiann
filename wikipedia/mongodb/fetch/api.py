from pymongo import MongoClient
import functools

import sentences


class WikiannAPI(object):
    """Wikiann API"""

    def __init__(self, host, port, db_name, collection_name, lang):
        self.client = MongoClient(host=host, port=port)
        self.collection = self.client[db_name][collection_name]
        self.lang = lang

    @functools.lru_cache(maxsize=None)
    def get_article_plain(self, kbid, paragraph_index=-1, space=True):
        """Given a Wikipedia title,
        return plain text of this article
        """

        kbid = kbid.replace('_', ' ')
        query = {
            'source_title': kbid,
        }
        if paragraph_index != -1:
            query['paragraph_index'] = paragraph_index
        res = ''
        for i in self.collection.find(query):
            sent = sentences.get_sent_plain(i, lower=False)
            if space:
                res += ' '.join(sent) + '\n'
            else:
                res += ''.join(sent) + '\n'
        return res

    @functools.lru_cache(maxsize=None)
    def get_anchor_links(self, kbid, paragraph_index=-1):
        """Given a Wikipedia title,
        return anchor links in this article
        """

        kbid = kbid.replace('_', ' ')
        query = {
            'source_title': kbid,
        }
        if paragraph_index != -1:
            query['paragraph_index'] = paragraph_index
        res = []
        for i in self.collection.find(query):
            for l in i['links']:
                res.append((l['title']))
        return res

    @functools.lru_cache(maxsize=None)
    def get_sentences_containing(self, kbid, limit=0):
        """Given a Wikipedia title,
        return the sentences containing anchor link to this title
        """

        kbid = kbid.replace('_', ' ')
        if self.lang == 'en':
            query = {'links.title': {'$in': [kbid]}}
        else:
            query = {'links.title_ll': {'$in': [kbid]}}
        return self.collection.find(query, limit=limit) # TO-DO return what?


if __name__ == '__main__':
    wikiann = WikiannAPI('0.0.0.0', 27017, 'enwiki', 'sentences', 'en')
    kbid = 'Great_horned_owl'
    for i in wikiann.get_sentences_containing(kbid, limit=10):
        print(i)
