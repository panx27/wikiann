import os
import re
import logging


logger = logging.getLogger()


class WikiParser:

    def __init__(self, lang):
        self.__config = {
            'unitok': self._unitok_init,
            'en': self._moses_init,
            'en_spacy': self._spacy_init,
            'en_stanford': self._stanford_init,
            'zh': self._zh_init,
            'gan': self._zh_init,
            'wuu': self._zh_init,
            'zh-yue': self._zh_init,
            'zh-classical': self._zh_init,
            'zh_ltp': self._ltp_init,
        }
        self._lang = lang

        logger.info('language: %s' % self._lang)
        if self._lang not in self.__config:
            logger.info('initializing default model: unitok')
            self.__config['unitok']()
        else:
            logger.info('initializing model: %s' % self._lang)
            self.__config[self._lang]()
        logger.info('done.')

    @staticmethod
    def _preserve_links(text, links):
        '''
        Preserve anchor text, e.g., '"Hello, World!" program'
        It may be split by sentence segmentor, replace whitespace with underline
        '''
        for link in links:
            b = link['start']
            e = link['end']
            text = text[:b] + text[b:e].replace(' ', '_') + text[e:]
        return text

    @staticmethod
    def _re_segment(text, index, sents, links):
        '''
        _preserve_links doesn't always work, we need to resegment sentences
        '''
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

    def _parse(self, text, index=0, links=None):
        if links:
            text = self._preserve_links(text, links)

        parsed_sents = []
        para_count = 0
        index = index
        for para in re.split('\n\s*\n', text):
            if not para:
                continue
            for line in para.split('\n'):
                if not line:
                    continue
                sents = self.sent_seger(line)

                if links:
                    sents = self._re_segment(text, index, sents, links)

                for sent in sents:
                    if self.processor:
                        r = self.processor(sent.replace('_', ' '))
                        toked_sent, processed_res = r
                    else:
                        toked_sent = self.tokenizer(sent.replace('_', ' '))
                        processed_res = {}
                    start = text.index(sent, index)
                    end = start + len(sent)
                    index = end
                    parsed_sents.append({
                        'tokens': toked_sent,
                        'start': start,
                        'end': end,
                        'processed': processed_res,
                        'paragraph_index': para_count
                    })
            para_count += 1
        return parsed_sents

    # Moses
    def _moses_init(self):
        from nltk.tokenize.moses import MosesTokenizer
        from nltk.tokenize import sent_tokenize
        self._punkt_model = sent_tokenize
        self._moses_model = MosesTokenizer(self._lang)

        self.parse = self._parse
        self.sent_seger = self._punkt_sent_seger
        self.tokenizer = self._moses_tokenizer
        self.processor = None

    def _punkt_sent_seger(self, text):
        return self._punkt_model(text)

    def _moses_tokenizer(self, text, shift=0):
        toked_sent = []
        index = 0
        for tok in self._moses_model.tokenize(text, escape=False):
            if tok.strip() == '':
                index += len(tok)
                continue
            tok_start = text.index(tok, index)
            tok_end = tok_start + len(tok)
            toked_sent.append({
                'text': tok,
                'start': tok_start + shift,
                'end': tok_end + shift
            })
            index = tok_end
        return toked_sent

    # Unitok
    def _unitok_init(self):
        from nltk.tokenize import sent_tokenize
        self._punkt_model = sent_tokenize
        from utils import unitok
        self._tokenize_model = unitok.unitok_tokenize

        self.parse = self._parse
        self.sent_seger = self._punkt_sent_seger
        self.tokenizer = self._unitok_tokenizer
        self.processor = None

    def _unitok_tokenizer(self, text, shift=0):
        toked_sent = []
        index = 0
        for tok in self._tokenize_model(text):
            if tok.strip() == '':
                index += len(tok)
                continue
            tok_start = text.index(tok, index)
            tok_end = tok_start + len(tok)
            toked_sent.append({
                'text': tok,
                'start': tok_start + shift,
                'end': tok_end + shift
            })
            index = tok_end
        return toked_sent

    # Chinese
    def _zh_init(self):
        import jieba
        jieba.initialize()
        self._jieba_model = jieba

        self.parse = self._parse
        self.sent_seger = self._zh_sent_seger
        self.tokenizer = self._zh_tokenizer
        self.processor = None

    def _zh_sent_seger(self, text):
        """
        use Chinese punctuation as delimiter
        """
        res = []
        sent_end_char = [u'。', u'！', u'？']
        current_sent = ''
        for i, char in enumerate(list(text)):
            if char in sent_end_char or i == len(list(text)) - 1:
                res.append(current_sent + char)
                current_sent = ''
            else:
                current_sent += char
        # return [item.strip() for item in res]
        return res

    def _zh_tokenizer(self, text, shift=0):
        toked_sent = []
        index = 0
        for tok in self._jieba_model.cut(text):
            if tok.strip() == '':
                index += len(tok)
                continue
            tok_start = text.index(tok, index)
            tok_end = tok_start + len(tok)
            toked_sent.append({
                'text': tok,
                'start': tok_start + shift,
                'end': tok_end + shift
            })
            index = tok_end
        return toked_sent

    # Spacy
    def _spacy_init(self):
        import spacy
        self._spacy_model = spacy.load('en') # TO-DO: self._lang?

        self.parse = self._parse
        self.sent_seger = self._spacy_sent_seger
        self.tokenizer = self._spacy_tokenizer
        self.processor = self._spacy_processor

    def _spacy_sent_seger(self, text):
        # TO-DO: running time
        return [s.text for s in self._spacy_model(text).sents]

    def _spacy_tokenizer(self, text, shift=0):
        toked_sent = []
        for t in self._spacy_model.tokenizer(text):
            if t.text.strip() == '':
                continue
            toked_sent.append({
                'text': t.text,
                'start': t.idx + shift,
                'end': t.idx + len(t) + shift
            })
        return toked_sent

    def _spacy_processor(self, text, shift=0):
        toked_sent = []
        processed_res = {}
        doc_obj = self._spacy_model(text)
        for t in doc_obj:
            if t.text.strip() == '':
                continue
            toked_sent.append((t.text, (t.idx+shift, t.idx+len(t)+shift)))
        processed_res['tree'] = doc_obj.print_tree()
        ner = []
        for i in doc_obj.ents:
            assert text[i.start_char:i.end_char] == i.text
            ner.append({
                'text': i.text,
                'offset': (i.start_char, i.end_char),
                'label': i.label_
            })
        processed_res['ner'] = ner
        return toked_sent, processed_res

    # LTP
    def _ltp_init(self):
        import pyltp
        # TO-DO: config file
        LTP_DATA_DIR = '/nas/data/m1/panx2/lib/ltp/ltp_data_v3.4.0'
        cws_model_path = os.path.join(LTP_DATA_DIR, 'cws.model')
        pos_model_path = os.path.join(LTP_DATA_DIR, 'pos.model')
        ner_model_path = os.path.join(LTP_DATA_DIR, 'ner.model')
        par_model_path = os.path.join(LTP_DATA_DIR, 'parser.model')

        self._splitter_model = pyltp.SentenceSplitter()
        self._segmentor_model = pyltp.Segmentor()
        self._segmentor_model.load(cws_model_path)
        self._postagger_model = pyltp.Postagger()
        self._postagger_model.load(pos_model_path)
        self._recognizer_model = pyltp.NamedEntityRecognizer()
        self._recognizer_model.load(ner_model_path)
        self._dparser_model = pyltp.Parser()
        self._dparser_model.load(par_model_path)

        self.parse = self._parse
        self.sent_seger = self._ltp_sent_seger
        self.tokenizer = self._ltp_tokenizer
        self.processor = self._ltp_processor

    def _ltp_sent_seger(self, text):
        return [s for s in self._splitter_model.split(text)]

    def _ltp_tokenizer(self, text, shift=0):
        # ltp cannot handle whitespace
        whitespaces = [' ', '\t', '\u3000']
        for ws in whitespaces:
            text = text.replace(ws, '，')

        toked_sent = []
        index = 0
        for tok in self._segmentor_model.segment(text):
            if tok.strip() == '':
                index += len(tok)
                continue
            tok_start = text.index(tok, index)
            tok_end = tok_start + len(tok)
            toked_sent.append({
                'text': tok,
                'start': tok_start + shift,
                'end': tok_end + shift
            })
            index = tok_end
        return toked_sent

    def _ltp_processor(self, text, shift=0):
        # ltp cannot handle whitespace
        whitespaces = [' ', '\t', '\u3000']
        for ws in whitespaces:
            text = text.replace(ws, '，')

        toked_sent = []
        index = 0
        tokens = self._segmentor_model.segment(text)
        for tok in tokens:
            if tok.strip() == '':
                index += len(tok)
                continue
            tok_start = text.index(tok, index)
            tok_end = tok_start + len(tok)
            toked_sent.append((tok, (tok_start+shift, tok_end+shift)))
            index = tok_end

        processed_res = {}
        postags = [p for p in self._postagger_model.postag(tokens)]
        netags = [n for n in self._recognizer_model.recognize(tokens, postags)]
        arcs = [(a.head, a.relation) \
                for a in self._dparser_model.parse(tokens, postags)]

        processed_res['pos'] = postags
        processed_res['ner'] = netags
        processed_res['arc'] = arcs
        return toked_sent, processed_res

    # Stanford (English)
    def _stanford_init(self):
        import stanfordnlp
        self._tokenize_model = stanfordnlp.Pipeline(lang='en',
                                                    processors='tokenize')
        self._nlp_model = stanfordnlp.Pipeline(lang='en')

        self.parse = self._parse
        self.sent_seger = self._stanford_sent_seger
        self.tokenizer = self._stanford_tokenizer
        self.processor = self._stanford_processor

    def _stanford_sent_seger(self, text):
        return [s.text for s in self.tokenize_model(text).sentences]
