import logging

from nltk.tokenize.moses import MosesTokenizer
from nltk.tokenize import sent_tokenize as punkt

from .base import Annotator


logger = logging.getLogger()


class MosesAnnotator(Annotator):
    """Moses Annotator
    Sentence segmentor: punkt from nltk
    Tokenization: Moses from nltk
    """

    def __init__(self, lang='en'):
        logger.info('initializing moses annotator...')
        super().__init__()
        self._moses = MosesTokenizer(lang)
        logger.info('done.')

    def _segment(self, text):
        return punkt(text)

    def _tokenize(self, text, shift=0):
        toks = []
        index = 0
        for tok in self._moses.tokenize(text, escape=False):
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
