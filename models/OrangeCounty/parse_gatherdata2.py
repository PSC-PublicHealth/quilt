#! /usr/bin/env python

import yaml
import json
import csv_tools
import types
import os
import os

with open('gatherdata2.csv', 'r') as f:
    keys, recs = csv_tools.parseCSV(f)

recDict = { r['# Abbrev']:r for r in recs}

fnames = []
for nm in os.listdir('facilityfacts'):
    if nm.endswith('.yaml'):
        fnames.append(nm[:-5])
print fnames

for nm in fnames:
    if nm not in recDict:
        print 'recDict missing %s' % nm

for nm in recDict.keys():
    if nm not in fnames:
        print 'fnames missing %s' % nm

for nm in recDict.keys():
    with open(os.path.join('facilityfacts2',nm+'.yaml'),'w') as of:
        if nm in fnames:
            with open(os.path.join('facilityfacts',nm+'.yaml'),'r') as infile:
                jsn = yaml.load(infile)
        else:
            jsn = {'abbrev': nm}
        rec = recDict[nm]
        jsn['meanPop'] = float(rec['N']) * float(rec['mean_LOS'])/365.0
        jsn['meanPop_prov'] = 'RHEA_Hosp-NH_Inputs_ADULT_2007_v03_06APR2012_properties_MRSA-STRAT-LOS.csv $B*$C/365'
        yaml.dump(jsn, of)

# for rec in recs:
#     print '%s' % type(rec['Bed Size'])
#     print '%s' % rec['Bed Size']
#     if isinstance(rec['Bed Size'], types.IntType):
#         nBeds = int(rec['Bed Size'])
#     else:
#         nBeds = None
#     jsn = {'name': rec['Name'],
#            'abbrev': rec['Abbrev'],
#            'nBeds': nBeds,
#            'nBeds_prov': '2007_Annual_Admissions__Bed_Sizes_TA DKsmb.xls'}
#     print jsn
#     with open('facilityfacts/%s.yaml' % jsn['abbrev'], 'w') as f:
#         yaml.dump(jsn, f)
