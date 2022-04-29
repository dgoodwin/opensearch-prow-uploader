#!/usr/bin/env python

import requests
import json
import os
import sys
import time
from itertools import islice
import tempfile

print("opening %s" % sys.argv[1])
f = open(sys.argv[1])
data = json.load(f)

# Iterate all the items in chunk:
chunk_size = 1000
iterator = iter(data["items"])
i = 0
while chunk := list(islice(iterator, chunk_size)):
    i += 1
    print("bulk upload request %s" % i)

    data_strs = []
    for item in chunk:
        index_line = {'index': {'_index': 'job1'}}
        data_strs.append(json.dumps(index_line))
        #transformed = transform(item)
        data_strs.append(json.dumps(item))
    data_strs.append("\n")

    data = "\n".join(data_strs)

    headers = {"Content-Type": "application/json"}
    r = requests.post(
            'https://search-dgoodwin-test-o4g3tsj6smjnfyxybu4m67ospy.us-east-1.es.amazonaws.com/_bulk',
            data=data,
            headers=headers,
            auth=('openshift', os.environ.get('OPENSEARCH_PASS'),
            )
    # headers = {"Content-Type": "application/json"}
    # r = requests.post(
            # 'https://search-dgoodwin-test-o4g3tsj6smjnfyxybu4m67ospy.us-east-1.es.amazonaws.com/job1/_doc',
            # data=json.dumps(item),
            # headers=headers,
            # auth=('openshift', 'miph3She9_'),
            # )

    # check status code for response received
    # success code - 200
    print(r)

    # print content of request
    print(r.content)

