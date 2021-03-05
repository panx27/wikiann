import logging

import requests
import ujson as json

from .base import Annotator


logger = logging.getLogger()


class TextsmartAnnotator(Annotator):
    """TextSmart Annotator
    Sentence segmentor: useing Chinese punctuation as delimiter
    Tokenization: n/a
    NLP: textsmart API
    """

    def __init__(self, lang='en'):
        logger.info('initializing textsmart annotator...')
        super().__init__()
        jieba.initialize()
        self._jieba = jieba
        self.nlp = self._nlp
        logger.info('done.')

    def _segment(self, text):
        sents = []
        sent_end_char = [u'。', u'！', u'？']
        current_sent = ''
        for i, char in enumerate(list(text)):
            if char in sent_end_char or i == len(list(text)) - 1:
                sents.append(current_sent + char)
                current_sent = ''
            else:
                current_sent += char
        return sents

    def _nlp(self, text, shift=0):
        obj = {"str": text}
        req_str = json.dumps(obj)
        url = "https://texsmart.qq.com/api"
        r = requests.post(url, data=req_str)
        r.encoding = "utf-8"
        res = json.loads(r.text)
        if 'word_list' not in res:
            return [], {}

        toks = []
        index = 0
        for word in res['word_list']:
            tok = word['str']
            if tok.strip() == '':
                index += len(tok)
                continue
            tok_start = text.index(tok, index)
            tok_end = tok_start + len(tok)
            toks.append({
                'text': tok,
                'start': tok_start + shift,
                'end': tok_end + shift
            })
            index = tok_end
        return toks, res
