import re


class Annotator:

    def __init__(self):
        self.segment = self._segment
        self.tokenize = self._tokenize
        self.nlp = None

    def annotate(self, text, index=0, links=None):
        if links:
            text = self._preserve_links(text, links)

        annotated_sents = []
        paragraph_count = 0
        index = index
        for para in re.split('\n\s*\n', text):
            if not para:
                continue
            for line in para.split('\n'):
                if not line:
                    continue
                sents = self.segment(line)

                if links:
                    sents = self._resegment(text, index, sents, links)

                for sent in sents:
                    if self.nlp:
                        toks, nlp_results = self.nlp(sent.replace('_', ' '))
                    else:
                        toks = self.tokenize(sent.replace('_', ' '))
                        nlp_results = {}
                    start = text.index(sent, index)
                    end = start + len(sent)
                    index = end
                    annotated_sents.append({
                        'tokens': toks,
                        'start': start,
                        'end': end,
                        'nlp': nlp_results,
                        'paragraph_index': paragraph_count
                    })
            paragraph_count += 1
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
    def _resegment(text, index, sents, links):
        """_preserve_links(**kwargs) doesn't always succeed, we need to
        resegment sentences.
        """
        new_sents = []
        n = 0
        index = index
        for i, sent in enumerate(sents):
            if n > i:
                continue
            start = text.index(sent, index)
            end = start + len(sent)
            index = end
            for link in links:
                while link['start'] >= start and link['start'] < end and \
                      link['end'] > end:
                    i += 1
                    if i >= len(sents):
                        break
                    sent += text[end:text.index(sents[i], end)+len(sents[i])]
                    end = start + len(sent)
            index = end
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
        nlp_results = {}
        return toks, nlp_results
