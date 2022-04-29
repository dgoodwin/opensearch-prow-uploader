#!/usr/bin/env python

import requests
import json
import os
import sys
import time
from itertools import islice
import tempfile

# morph various types of data into our common pattern as much as possible.
# some documents will have additional properties but we target a core common set.
def transform(item):
    if 'kind' in item and item['kind'] == 'Event':
        # transforms for a Kube event:
        item['from'] = item['firstTimestamp']
        item.pop('firstTimestamp')
        item['to'] = item['lastTimestamp']
        item.pop('lastTimestamp')
        if 'type' in item:
            item['level'] = item['type']
            item.pop('type')
        locator_tokens = []
        for k in item['source']:
            locator_tokens.append("%s/%s" % (k, item['source'][k]))
        item.pop('source')
        item['locator'] = " ".join(locator_tokens)
    return item

if __name__ == "__main__":
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
            transformed = transform(item)
            data_strs.append(json.dumps(transformed))
        data_strs.append("\n")

        data = "\n".join(data_strs)

        headers = {"Content-Type": "application/json"}
        r = requests.post(
                'https://search-dgoodwin-test-o4g3tsj6smjnfyxybu4m67ospy.us-east-1.es.amazonaws.com/_bulk',
                data=data,
                headers=headers,
                auth=('openshift', os.environ.get('OPENSEARCH_PASS')),
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

