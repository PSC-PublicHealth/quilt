#! /usr/bin/env python

import yaml_tools

allKeySet, recs = yaml_tools.parse_all('facilityfacts6')

for rec in recs:
    if rec['nBeds_prov'] == 'meanPop * 1.1':
        del rec['nBeds']
        del rec['nBeds_prov']
    rec['fracAdultPatientDaysICU'] = rec['fracBedsICU']
    rec['fracAdultPatientDaysICU_prov'] = rec['fracBedsICU_prov']
    del rec['fracBedsICU']
    del rec['fracBedsICU_prov']

yaml_tools.save_all('facilityfacts7', recs)
