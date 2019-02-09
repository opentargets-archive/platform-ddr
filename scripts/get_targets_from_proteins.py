import requests
import csv
import addict
import json
import itertools as iters
import functional as fn
import more_itertools as miters
import sys

ptog = {}
with open('omnipath.tsv','r') as f:
    # compute protein - target dict
    for i, d in enumerate(miters.chunked(csv.DictReader(f, fieldnames=['object', 'subject', 'score'], dialect='excel-tab'),100)):
        elems = fn.seq(d).map(lambda el: [el['object'], el['subject']]).flatten().to_list()
        r = requests.post('https://platform-api.opentargets.io/v3/platform/private/besthitsearch', json = {'q':elems})

        for el in r.json()['data']:
            eld = addict.Dict(el)
            ptog[eld.q] = eld.data.name

        sys.stdout.flush()
        print str(i*100)
print 'done'

with open('omnipath.tsv','r') as f:
    # generate proper dicts
    for d in csv.DictReader(f, fieldnames=['object', 'subject', 'score'], dialect='excel-tab'):
        try:
            rr = {'object': ptog[d['object']], 'subject': ptog[d['subject']], 'score': 1.0 if d['score'] == '1' else 0.25}

            print(json.dumps(rr))
        except:
            pass
