import gzip
import re
import argparse
import logging


'''
 From page.sql.gz

CREATE TABLE `page` (
  `page_id` int(8) unsigned NOT NULL AUTO_INCREMENT,
  `page_namespace` int(11) NOT NULL DEFAULT '0',
  `page_title` varbinary(255) NOT NULL DEFAULT '',
  `page_restrictions` tinyblob NOT NULL,
  `page_counter` bigint(20) unsigned NOT NULL DEFAULT '0',
  `page_is_redirect` tinyint(1) unsigned NOT NULL DEFAULT '0',
  `page_is_new` tinyint(1) unsigned NOT NULL DEFAULT '0',
  `page_random` double unsigned NOT NULL DEFAULT '0',
  `page_touched` varbinary(14) NOT NULL DEFAULT '',
  `page_links_updated` varbinary(14) DEFAULT NULL,
  `page_latest` int(8) unsigned NOT NULL DEFAULT '0',
  `page_len` int(8) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`page_id`),
  UNIQUE KEY `name_title` (`page_namespace`,`page_title`),
  KEY `page_random` (`page_random`),
  KEY `page_len` (`page_len`),
  KEY `page_redirect_namespace_len` (`page_is_redirect`,`page_namespace`,`page_len`)
)
'''


logger = logging.getLogger()


def get_mapping_id2title_page(pdata):
    f = gzip.open(pdata, 'rt')
    res = {}
    for line in f:
        line = line.replace('INSERT INTO `page` VALUES (', '')
        for i in line.strip().split('),('):
            # Only select namespace 0 (Main/Article) pages
            m = re.match('(\d+),0,\'(.*?)\',\'', i)
            if m != None:
                page_id = m.group(1)
                page_title = m.group(2)
                try:
                    assert page_id not in res
                except:
                    msg = 'Duplicated id: %s\t%s\t%s\t%s' % (page_id,
                                                             res[page_id],
                                                             page_title,
                                                             i)
                    logger.error(msg)
                res[page_id] = page_title
    return res


def get_mapping_title2id_page(pdata):
    f = gzip.open(pdata, 'rt')
    res = {}
    for line in f:
        line = line.replace('INSERT INTO `page` VALUES (', '')
        for i in line.strip().split('),('):
            # Only select namespace 0 (Main/Article) pages
            m = re.match('(\d+),0,\'(.*?)\',\'', i)
            if m != None:
                page_id = m.group(1)
                page_title = m.group(2)
                try:
                    assert page_title not in res
                except:
                    msg = 'Duplicated title: %s\t%s\t%s\t%s' % (page_title,
                                                                res[page_title],
                                                                page_id,
                                                                i)
                    logger.error(msg)
                res[page_title] = page_id
    return res


'''
 From pages-articles-multistream-index.txt
'''
def get_mapping_title2id_index(pdata):
    titles = {}
    for line in open(pdata, 'r', 'utf-8'):
        m = re.match('(\d+)\:(\d+)\:(.+)', line)
        s = m.group(1)
        id_ = m.group(2)
        title = m.group(3)
        try:
            assert title not in titles
        except:
            logger.error('Duplicated id: %s\t%s\t%s' % \
                         (id_, titles[title], title))
        titles[title] = id_
    return titles


def get_mapping_id2title_index(pdata):
    titles = {}
    for line in open(pdata, 'r', 'utf-8'):
        m = re.match('(\d+)\:(\d+)\:(.+)', line)
        s = m.group(1)
        id_ = m.group(2)
        title = m.group(3)
        try:
            assert id_ not in titles
        except:
            logger.error('Duplicated id: %s\t%s\t%s' % \
                         (id_, titles[id_], title))
        titles[id_] = title
    return titles


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('page', help='path to wiki-page.sql.gz')
    parser.add_argument('output', help='output path')
    args = parser.parse_args()

    id2title = get_mapping_id2title_page(args.page)
    with open(args.outpath, 'w') as fw:
        for id_ in sorted(id2title.keys()):
            title = id2title[id_].replace('\\', '').replace(' ', '_')
            fw.write('%s\t%s\n' % (id_, title))
