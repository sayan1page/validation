import json
import sys
import  MySQLdb

res = {}
for t in ['li','result']:
    res[t] = {}
    base = 2
    exponant = 8
    offset = 1
    lim = (base ** exponant) + offset
    if t == 'result':
        res1 = []
        for i in range(lim):
            res1.append(1)
        res[t] = res1
    if t == "li":
        res1 = []
        for i in  range(lim):
            res1.append({"keys": ["sid"], "values" :[["577068"]]})
            res[t] = res1

publishers = []    
f = open('publisher.csv')
for line in f:
    p = line.strip()
    if p not in publishers and p != '':
        publishers.append(int(p))
f.close()
for p in publishers:
    print(str(p) + "\t" +str(res))
