import pytest
import sys
sys.path.append('../resource')
from MySqlDb import MysqlDbWrapper



def pytest_generate_tests(metafunc):
    left = []
    right = []
    case = []
    mysql_instance = MysqlDbWrapper('../../config/map4.config')
    f = open('../../config/map4.config')
    config = {}
    for line in f:
        fields = line.strip().split('=')
        config[fields[0]] = fields[1].strip()
    f.close()
    table_name = config['pytest_table_name']
    left_column_name = config['pytest_left_column_name']
    right_column_name = config['pytest_right_column_name']
    case_column_name = config['pytest_case_column_name']
    query = "select " + left_column_name + "," + right_column_name + "," + case_column_name + " from " + table_name
    pytest_data = mysql_instance.get_data(query)
    for row in pytest_data:
        left.append(str(row[0]))
        right.append(str(row[1]))
        case.append(str(row[2]))
    idlist = case
    argnames = ['left','right']
    argvalues = []
    for i in range(len(left)):
        argvalues.append([left[0],right[0]])
    metafunc.parametrize(argnames, argvalues, ids=idlist, scope="class")



class TestTakeRateByPlatformFee:

    def test_valid_take_rate_by_pfee(self, left, right):
        assert float(left) <= float(right)
        assert float(left) >= 0.2 * float(right)
