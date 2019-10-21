import logging

import unicodedata as ud
from nltk.tokenize import sent_tokenize as punkt

from .base import Annotator


logger = logging.getLogger()


class UnitokAnnotator(Annotator):
    """Universal Annotator
    Sentence segmentor: punkt from nltk
    Tokenization: unitok by Jonathan May (jonmay@isi.edu)
    """

    def __init__(self):
        logger.info('initializing unitok annotator...')
        super().__init__()
        logger.info('done.')

    def _segment(self, text):
        return punkt(text)

    def _tokenize(self, text, shift=0):
        print(text)
        toks = []
        index = 0
        for tok in self.unitok_tokenize(text):
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

    @staticmethod
    def unitok_tokenize(data):
        toks = []
        for offset, char in enumerate(data):
            cc = ud.category(char)
            if char in ['ʼ', '’', '‘', '´', '′', "'"]:
                toks.append(char)
            elif cc.startswith("P") \
                 or cc.startswith("S") \
                 or char in ['።', '፡']:
                toks.append(' ')
                toks.append(char)
                toks.append(' ')
            else:
                toks.append(char)
        toks = [item for item in ''.join(toks).split() if item]
        return toks
