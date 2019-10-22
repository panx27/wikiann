import logging

import jieba

from .base import Annotator


logger = logging.getLogger()


class ChineseAnnotator(Annotator):
    """Chinese Annotator
    Sentence segmentor: useing Chinese punctuation as delimiter
    Tokenization: jieba
    """

    def __init__(self, lang='en'):
        logger.info('initializing chinese annotator...')
        super().__init__()
        jieba.initialize()
        self._jieba = jieba
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

    def _tokenize(self, text, shift=0):
        toks = []
        index = 0
        for tok in self._jieba.cut(text):
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
        return toks
