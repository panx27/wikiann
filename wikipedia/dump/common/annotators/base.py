import re
import logging


logger = logging.getLogger()


class Annotator:

    def __init__(self):
        self.segment = self._segment
        self.tokenize = self._tokenize
        self.nlp = None

    def annotate(self, text, shift=0, links=None):
        if links:
            text = self._preserve_links(text, links)

        shift = shift
        paragraph_index = 0
        annotated_sents = []
        for n, para in enumerate(re.split('\n', text)):
            if not para.strip():
                continue

            sents = self.segment(para)
            if links:
                sents = self._resegment(text, shift, sents, links)

            for sent in sents:
                if self.nlp:
                    try:
                        toks, nlp_res = self.nlp(sent.replace('_', ' '))
                    except Exception as e:
                        logger.error('unexpected error')
                        logger.error(e)
                        logger.error(sent.replace('_', ' '))
                else:
                    toks = self.tokenize(sent.replace('_', ' '))
                    nlp_res = {}
                start = text.index(sent, shift)
                end = start + len(sent)
                shift = end
                annotated_sents.append({
                    'tokens': toks,
                    'start': start,
                    'end': end,
                    'nlp': nlp_res,
                    'paragraph_index': paragraph_index
                })
            paragraph_index += 1
        return annotated_sents

    @staticmethod
    def _preserve_links(text, links):
        """Preserve anchor text.
        e.g., anchor text `"Hello, World!" program` may be split
        by some sentence segmentors. Hence, replace whitespace with underline
        """
        for link in links:
            b = link['start']
            e = link['end']
            text = text[:b] + text[b:e].replace(' ', '_') + text[e:]
        return text

    @staticmethod
    def _resegment(text, shift, sents, links):
        """_preserve_links(**kwargs) doesn't always succeed, we need to
        resegment sentences.
        """
        new_sents = []
        n = 0
        shift = shift
        for i, sent in enumerate(sents):
            if n > i:
                continue
            start = text.index(sent, shift)
            end = start + len(sent)
            shift = end
            for link in links:
                while link['start'] >= start and link['start'] < end and \
                      link['end'] > end:
                    i += 1
                    if i >= len(sents):
                        break
                    sent += text[end:text.index(sents[i], end)+len(sents[i])]
                    end = start + len(sent)
            shift = end
            n = i + 1
            new_sents.append(sent)
        return new_sents

    def _segment(self, text):
        """Sentence segmentation
        Split string into a list of string.
        """
        sents = []
        return sents

    def _tokenize(self, text):
        """Tokenization
        Split string into a list of string.
        """
        toks = []
        return toks

    def _nlp(self, text):
        """NLP models
        Store results into a dict.
        """
        toks = []
        nlp_res = {}
        return toks, nlp_res
