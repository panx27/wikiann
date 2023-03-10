import os
import sys
import re


indir = sys.argv[1]
for i in os.listdir(indir):
    if not re.search('(\w+)wiki\-', i):
        continue

    for j in os.listdir('%s/%s/' % (indir, i)):
        if os.path.isdir('%s/%s/%s/' % (indir, i, j)):
            print('%s/%s/%s/' % (indir, i, j))
