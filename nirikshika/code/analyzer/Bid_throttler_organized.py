import pytest
import pandas as pd
import sys
sys.path.append('/opt/Utils/Modules/')
import configReader_python3 as configReader



def get_config(sectionName):
    config = configReader.configReader()
    config_path = "/opt/QA/ML/regular_throttling/regular_bid_throttle_config.ini"
    srcVal = config.getSectionDict(config_path,sectionName)
    return srcVal


test_path = get_config("input_output_path")
output_path = test_path["output_path"]


def pytest_generate_tests(metafunc):
    argnames = ['country', 'pub_id','site_id']
    argvalues = []
    data = pd.read_csv(output_path)
    for index,row in data.iterrows():
        argvalues.append([row["gctry"],row["publisher_id"],row["sid"]])

    metafunc.parametrize(argnames, argvalues, scope="class")
		
class TestNullSiteIdPubId:


    def test_is_null(self,country,pub_id,site_id):
        assert str(country) != ""
        assert str(pub_id)  != ""
        assert str(site_id) != ""
