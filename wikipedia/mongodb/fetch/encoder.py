class Encoder(object):
    langs = ['zh', 'zh-classical']

    def __init__(self, lang):
        self.encoders = {
            'zh': self.to_simplified,
            'zh-classical': self.to_simplified,
        }
        self.lang = lang
        if self.lang in ['zh', 'zh-classical']:
            import zhconv
            self.convert = zhconv.convert


    def encoding(self, text):
        if self.lang in self.encoders:
            return self.encoders[self.lang](text)
        else:
            return text


    def to_simplified(self, text):
        return self.convert(text, 'zh-cn')


if __name__ == '__main__':
    encoder = Encoder('zh')
    print(encoder.encoding('乾坤一擲'))
    print(encoder.encoding('滙豐銀行'))
