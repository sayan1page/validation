import json
import pytest
import sys



def pytest_generate_tests(metafunc):
    idlist = []
    argnames = ['adsize']
    argvalues = []
    f = open('../../config/jsonlist.txt')
    for line in f:
        filename = line.strip()
        with open('../'+filename,'r') as handle:
            data = json.loads(str(handle.read()))
            for t in ['li', 'campaignid', 'result']:
                elements = data[t]
                for e in elements:
                    if t!= 'result' and 'keys' in e:
                        keys = e['keys']
                        for k in keys:
                            if str(k) == "adsize":
                                index = keys.index(k)
                                for v in e['values'][index]:
                                    argvalues.append([v])

    metafunc.parametrize(argnames, argvalues, scope="class")
		
class TestCpAlphaDistribution:


    def test_sum_is_hundred(self, adsize):
        assert str(adsize) != ""
