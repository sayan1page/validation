import json
import pytest
import sys



def pytest_generate_tests(metafunc):
    idlist = []
    argnames = ['sum_i','sum_pc']
    argvalues = []
    f = open('input.txt')
    for line in f:
        filename = line.strip()
        with open('/home/ml.user/QA/Sayan/map4json/'+filename,'r') as handle:
            data = json.loads(str(handle.read()))
            for el in data['cp']:
                for k in el['alpha']:
                    sum_i = 0
                    sum_pc = 0
                    for k1 in k['arm']:
                        sum_i = sum_i + k1['i']
                        sum_pc = sum_pc + k1['pc']
                    idlist.append(filename + ':' + str(el['start']) + ':'+ str(el['alpha_len']) +':'+ str(k['start']))
                    #idlist.append(filename + ':' + str(el['start']) + ':'+ str(el['alpha_len']) +':'+ str(k['start']) + ':'+ str(k['arm_ratio_len']))
                    argvalues.append([sum_i,sum_pc])
    metafunc.parametrize(argnames, argvalues, ids=idlist, scope="class")
		
class TestCpAlphaDistribution:


    def test_sum_is_hundred(self, sum_i, sum_pc):
        assert int(sum_pc) == 1000000 or  int(sum_pc)== 999999
