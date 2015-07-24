#! /usr/bin/env python

import yaml
import json
import csv_tools
import types
import os
import os


def unicode_safe_constructor(loader, node):
    return node.value

yaml.SafeLoader.add_constructor("tag:yaml.org,2002:python/unicode",
                                unicode_safe_constructor)


def loadYamlDir(dirName):
    recs = []
    for nm in os.listdir(dirName):
        if nm.endswith('.yaml'):
            with open(os.path.join(dirName, nm), 'r') as f:
                recs.append(yaml.safe_load(f))
    print [r['abbrev'] for r in recs]
    return recs


with open('gatherdata3.csv', 'r') as f:
    keys, recs = csv_tools.parseCSV(f)
for r in recs:
    for fld in r:
        if isinstance(r[fld], types.UnicodeType):
            r[fld] = r[fld].encode('utf-8')
recDict = {r['abbrev']: r for r in recs}

existingRecs = loadYamlDir('facilityfacts3')
fnames = []
for nm in os.listdir('facilityfacts3'):
    if nm.endswith('.yaml'):
        fnames.append(nm[:-5])
print fnames

noAbbrevCtr = 0
for eRec in existingRecs:
    abbrev = eRec['abbrev']
    if abbrev in recDict:
        print 'ping %s' % abbrev
        assert eRec['category'] == 'HOSPITAL', '%s is not a HOSPITAL' % abbrev
        r = recDict[abbrev]
        fracBedsICU = r['fracBedsICU']
        if isinstance(fracBedsICU, types.FloatType):
            eRec['fracBedsICU'] = fracBedsICU
            eRec['fracBedsICU_prov'] = 'ICU Estimated LOS 7-22-09.xls $B'
        icuLOS = r['ICULOSMean']
        if isinstance(icuLOS, types.FloatType):
            eRec['meanLOSICU'] = {'value': icuLOS,
                                  'prov': 'ICU Estimated LOS 7-22-09.xls $D',
                                  'startdate': None,
                                  'enddate': None
                                  }
            print eRec

    if 'abbrev' in eRec and len(eRec['abbrev']) > 0:
        ofname = eRec['abbrev'] + '.yaml'
    else:
        ofname = 'none%d.yaml' % noAbbrevCtr
        noAbbrevCtr += 1
    with open(os.path.join('facilityfacts4', ofname), 'w') as f:
        yaml.dump(eRec, f,
                  default_flow_style=False, indent=4,
                  encoding='utf-8', width=130, explicit_start=True)

