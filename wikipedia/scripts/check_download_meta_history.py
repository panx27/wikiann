import os
import sys
import urllib.request


input_path = sys.argv[1]
with open(input_path, "r") as f:
    for line in f:
        tmp = line.rstrip("\n").split()
        url = tmp[1]
        file_path = tmp[-1]
        req = urllib.request.Request(url, method='HEAD')
        f = urllib.request.urlopen(req)
        url_size = int(f.headers['Content-Length'])
        file_size = os.path.getsize(file_path)
        if url_size != file_size:
            print(f"size not match: {url_size} {file_size}")
