import logging


logger = logging.getLogger()

CHINESE_LANGUAGES = ['zh', 'gan', 'wuu', 'zh-yue', 'zh-classical']


def get_annotator(lang):
    logger.info('language: %s' % lang)

    if lang == 'unitok':
        from .annotators.unitok import UnitokAnnotator
        return UnitokAnnotator()

    elif lang == 'en':
        from .annotators.moses import MosesAnnotator
        return MosesAnnotator()

    elif lang in CHINESE_LANGUAGES:
        from .annotators.chinese import ChineseAnnotator
        return ChineseAnnotator()

    else:
        from .annotators.unitok import UnitokAnnotator
        return UnitokAnnotator()
