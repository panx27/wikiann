import logging


logger = logging.getLogger()

CHINESE_LANGUAGES = ['zh', 'gan', 'wuu', 'zh-yue', 'zh-classical']


def get_annotator(lang):
    logger.info('language: %s' % lang)

    if lang == 'unitok':
        from .annotators.unitok import UnitokAnnotator
        return UnitokAnnotator()

    elif lang in CHINESE_LANGUAGES:
        from .annotators.chinese import ChineseAnnotator
        return ChineseAnnotator()

    elif lang == 'zh_textsmart':
        from .annotators.textsmart import TextsmartAnnotator
        return TextsmartAnnotator()

    else:
        from .annotators.unitok import UnitokAnnotator
        return UnitokAnnotator()
