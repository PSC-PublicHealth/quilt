#! /usr/bin/env python

import yaml
import json
import csv_tools
import types
import os
import os.path
import sys
import math


def unicode_safe_constructor(loader, node):
    return node.value


yaml.SafeLoader.add_constructor("tag:yaml.org,2002:python/unicode",
                                unicode_safe_constructor)

recs = []
dirnm = sys.argv[1]
for nm in os.listdir(sys.argv[1]):
    if nm.endswith('.yaml'):
        with open(os.path.join(dirnm, nm), 'r') as f:
            recs.append(yaml.safe_load(f))
print [r['abbrev'] for r in recs]

noAbbrevCtr = 0
for rec in recs:
    for fld in rec:
        if isinstance(rec[fld], types.UnicodeType):
            rec[fld] = rec[fld].encode('utf-8')
    printMe = False
    msg = ''
    if 'category' not in rec:
        printMe = True
        msg += 'no category;'
    if 'nBeds' not in rec:
        rec['nBeds'] = None
        printMe = True
        msg += 'no nBeds;'
    if not rec['nBeds']:
        rec['nBeds_prov'] = None
        if 'category' in rec and rec['category'] == 'HOSPITAL' and 'meanPop' in rec:
            rec['nBeds'] = int(math.floor(1.1*rec['meanPop']))
            rec['nBeds_prov'] = 'meanPop * 1.1'
        else:
            printMe = True
            msg += 'empty nBeds;'
    if len(rec['abbrev']) == 0:
        printMe = True
        msg += 'no abbrev;'
    if 'fracBedsICU' not in rec and 'category' in rec and rec['category'] == 'NURSINGHOME':
        rec['fracBedsICU'] = 0.0
        rec['fracBedsICU_prov'] = "Nursing homes don't have ICUs"
    if printMe:
        print '-------\n- %s' % msg
        print yaml.dump(rec, default_flow_style=False, indent=4,
                        encoding='utf-8', width=130, explicit_start=True)

    if 'abbrev' in rec and len(rec['abbrev']) > 0:
        ofname = rec['abbrev'] + '.yaml'
    else:
        ofname = 'none%d.yaml' % noAbbrevCtr
        noAbbrevCtr += 1
    with open(os.path.join(sys.argv[2], ofname), 'w') as f:
        yaml.dump(rec, f,
                  default_flow_style=False, indent=4,
                  encoding='utf-8', width=130, explicit_start=True)


