#! /usr/bin/env python

import yaml
import types
import os.path
import sys
import math
from collections import OrderedDict
import csv_tools

import yaml_ordered
yaml_ordered.install()


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

with open('gatherdata4.csv', 'r') as f:
    csvKeys, csvRecs = csv_tools.parseCSV(f)
for r in csvRecs:
    for fld in r:
        if isinstance(r[fld], types.UnicodeType):
            r[fld] = r[fld].encode('utf-8')
csvRecDict = {r['# Abbrev']: r for r in csvRecs}

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
    for k in ['meanLOS', 'meanLOSICU']:
        if k in rec:
            subR = rec[k]
            for subK in ['startdate', 'enddate']:
                if subR[subK] is None:
                    del subR[subK]
    if ('fracBedsICU' in rec and rec['fracBedsICU'] == 0.0
            and 'meanLOSICU' not in rec):
        rec['meanLOSICU'] = {'value': 0.0, 'prov': 'No ICU present'}
    if (rec['category'] == 'HOSPITAL' and 'meanLOSICU' not in rec):
        rec['meanLOSICU'] = {'value': 7.7,
                             'prov': 'Mean of known values from ICU Estimated LOS 7-22-09'}
    if (rec['category'] == 'HOSPITAL' and 'fracBedsICU' not in rec):
        rec['fracBedsICU'] = 0.108
        rec['fracBedsICU_prov'] = 'Mean of known values from ICU Estimated LOS 7-22-09'
    if 'meanLOS' not in rec and rec['abbrev'] in csvRecDict:
        rec['meanLOS'] = {'value': float(csvRecDict[rec['abbrev']]['mean_LOS']),
                          'prov': 'RHEA_Hosp-NH_Inputs_ADULT_2007_v03_06APR2012_properties_MRSA-STRAT-LOS.csv $C'}

    if printMe:
        print '-------\n- %s' % msg
        print yaml.dump(rec, default_flow_style=False, indent=4,
                        encoding='utf-8', width=130, explicit_start=True)

# Re-copy everything, but with ordered keys
newRecs = []
for rec in recs:
    newRec = OrderedDict()
    for k in ['name', 'abbrev']:
        newRec[k] = rec[k]
    for k in rec:
        if k not in ['name', 'abbrev']:
            newRec[k] = rec[k]
    newRecs.append(newRec)

# Write output
for rec in newRecs:
    if 'abbrev' in rec and len(rec['abbrev']) > 0:
        ofname = rec['abbrev'] + '.yaml'
    else:
        ofname = 'none%d.yaml' % noAbbrevCtr
        noAbbrevCtr += 1
    with open(os.path.join(sys.argv[2], ofname), 'w') as f:
        yaml.safe_dump(rec, f,
                       default_flow_style=False, indent=4,
                       encoding='utf-8', width=130, explicit_start=True)
