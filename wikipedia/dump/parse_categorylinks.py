import sys
import os
import re
import logging
import gzip
import argparse
from collections import defaultdict

import ujson as json


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def normalize_title(title):
    # Remove backslash
    title = title.replace('\\', '')
    # Replace underline
    title = title.replace('_', ' ')
    return title


def process(input_path, output_path):
    """From langlinks.sql.gz

    CREATE TABLE `categorylinks` (
    `cl_from` int(8) unsigned NOT NULL DEFAULT 0,
    `cl_to` varbinary(255) NOT NULL DEFAULT '',
    `cl_sortkey` varbinary(230) NOT NULL DEFAULT '',
    `cl_timestamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
    `entry[4]` varbinary(255) NOT NULL DEFAULT '',
    `cl_collation` varbinary(32) NOT NULL DEFAULT '',
    `cl_type` enum('page','subcat','file') NOT NULL DEFAULT 'page',
    PRIMARY KEY (`cl_from`,`cl_to`),
    KEY `cl_timestamp` (`cl_to`,`cl_timestamp`),
    KEY `cl_sortkey` (`cl_to`,`cl_type`,`cl_sortkey`,`cl_from`),
    KEY `cl_collation_ext` (`cl_collation`,`cl_to`,`cl_type`,`cl_from`)
    )
    """

    # pattern = r"\(\d+,'.*?','.*?','\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}','.*?','.*?','.*?'\)"
    pattern = r"\((\d+),'(.*?)','(.*?)','(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})','(.*?)','(.*?)','(.*?)'\)"

    logger.info(f'loading {input_path}')
    logger.info('processing...')
    with gzip.open(input_path, 'rb') as f, open(output_path, "w") as fw:
        for line in f:
            line = line.decode('utf-8', 'ignore')

            entries = re.findall(pattern, line)
            for entry in entries:
                d = {
                    'category': normalize_title(entry[1]),
                    'type': entry[6],
                }
                title = entry[2]
                prefix = entry[4]
                if prefix:
                    title = title[len(prefix)+2:]
                d['title'] = normalize_title(title)
                d['raw'] = entry
                fw.write(json.dumps(d, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', '-i')
    parser.add_argument('--output_path', '-o')
    args = parser.parse_args()

    process(args.input_path, args.output_path)
