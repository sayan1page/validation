import json
import pytest
import sys

sys.path.append('../resource')
from MySqlDb import MysqlDbWrapper



def pytest_generate_tests(metafunc):
    f = open("/home/ml.user/QA/Sayan/nirikshmika/config/bidthrottle.config_json")
    config = {}
    for line in f:
        fields = line.strip().split('=')
        config[fields[0]] = fields[1].strip()
    f.close()
    table_name = config['output_table_name']
    mysql_instance = MysqlDbWrapper("/home/ml.user/QA/Sayan/nirikshmika/config/bidthrottle.config_json")
    rows = mysql_instance.get_data("select pub_id,targeting_info from pub_bid_probability_info_1")
    argnames = ['key','value']
    argvalues = []
    for row in rows:
        data = json.loads(row[1])
        pid = row[0]
        for t in ['li', 'campaignid', 'result','isCookied']:
            elements = None
            if t in data:
                elements = data[t]
            if t == 'result' and elements is None:
                raise Exception("Rsult is None for pid " + str(pid))
            if elements is not None:
                for e in elements:
                    if t == 'li':
                        keys = e['keys']
                        values = e['values']
                        if len(keys) != len(values):
                            raise Exception("No of Key and Values are not same in pid " + str(pid))
                        for i in range(len(keys)):
                            argvalues.append([keys[i],values[i]])

    metafunc.parametrize(argnames, argvalues, scope="class")


		
class TestBidThthrottle:


    def test_key_value(self,key,value):
        assert isinstance(key, str) == True
        assert isinstance(value, list) == True
        assert len(value) >= 1
        assert key != ""
        assert value != ""

