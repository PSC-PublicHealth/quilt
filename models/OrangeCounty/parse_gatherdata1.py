#! /usr/bin/env python

import yaml
import json
import csv_tools
import types

with open('gatherdata1.csv', 'r') as f:
    keys, recs = csv_tools.parseCSV(f)

for rec in recs:
    if isinstance(rec['Bed Size'], types.IntType):
        nBeds = int(rec['Bed Size'])
    else:
        nBeds = None
    jsn = {'name': rec['Name'],
           'abbrev': rec['Abbrev'],
           'nBeds': nBeds,
           'nBeds_prov': '2007_Annual_Admissions__Bed_Sizes_TA DKsmb.xls',
           'category': rec['Category']}
    print jsn
    with open('facilityfacts/%s.yaml' % jsn['abbrev'], 'w') as f:
        yaml.dump(jsn, f)
