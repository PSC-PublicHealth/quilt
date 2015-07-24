#! /usr/bin/env python

"""
This assumes that address data has been manually added to the yaml files, and uses Google's geocoding
API to add latitude and longitude.
"""

import urllib
import json
import time
import yaml_tools

allKeySet, recs = yaml_tools.parse_all('facilityfacts8')

newRecs = []

for rec in recs:
    assert 'address' in rec
    assert 'address_prov' in rec
    if 'latitude' in rec and 'longitude' in rec:
        print '%s already done' % rec['abbrev']
    else:
        try:
            query = {'address' : rec['address']}
            url = ('http://maps.googleapis.com/maps/api/geocode/json?address=%s' %
                   urllib.urlencode(query))
        except Exception,e:
            print 'bad encode: %s' % e
            print 'query: %s' % query
            print 'url: %s' % url
            continue
        try:
            data = None
            time.sleep(1.0)
            response = urllib.urlopen(url)
            data = json.loads(response.read())
        except Exception, e:
            print 'bad transaction with googleapis: %s' % e
            print 'returned JSON: %s' % data
            continue
        resVec = data['results']
        if rec['abbrev'] == 'GGMC' and len(resVec) == 2:
            # GGMC has two hits at the same address
            resVec = resVec[:1]
        if len(resVec) == 1:
            latitude = resVec[0]['geometry']['location']['lat']
            longitude = resVec[0]['geometry']['location']['lng']
            nR = rec.copy()
            nR['latitude'] = latitude
            nR['longitude'] = longitude
            nR['lat_lon_prov'] = 'maps.googleapis.com/maps/api/geocode on address'
            print '%s -> %s %s' % (nR['abbrev'], nR['latitude'], nR['longitude'])
            newRecs.append(nR)
        else:
            print 'bad geocode for %s; data follows' % rec['abbrev']
            print data
            print 'Results in order:'
            for res in resVec:
                print res

print '%d records modified' % len(newRecs)
yaml_tools.save_all('facilityfacts9', newRecs)
