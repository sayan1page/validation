import sys
sys.path.append('/opt/Utils/Modules/')
import configReader_python3 as configReader
import pandas as pd
import subprocess
import ast
sys.path.append("/home/ml.user/QA/Dhananjay/bid_throtteling/computation/regular_throttling/MAD-1536_modified")
from create_buckets_utils import *
import csv

def get_config(sectionName):
    config = configReader.configReader()
    config_path = "/opt/QA/ML/regular_throttling/regular_bid_throttle_config.ini"
    srcVal = config.getSectionDict(config_path,sectionName)
    return srcVal

test_path = get_config("input_output_path")
input_path = test_path["input_path"]
output_path = test_path["output_path"]

path = get_config("ML_path")
ML_path = path["ml_path"]
prod_config = read_db_config(ML_path)

#1
def test_map_countrycode_reverse():
    src_val = get_config("test_map_countrycode_reverse")
    expected_country_code = src_val["country_code"]
    expected_country_id = src_val["id"]
    country_code = map_countrycode_reverse(prod_config)
    for curr_key, curr_val in country_code.items():
        if curr_key == expected_country_code:
            assert curr_key == expected_country_code
            assert curr_val == int(expected_country_id)


#2
def test_num_columns_in_input_file():
    srcVal = get_config("test_num_columns_in_input_file")
    expected_result = srcVal["expected_result"]
    awk_command = "cat %s | awk -F ',' '{print NF}' | sort | uniq"%(input_path)
    p1 = subprocess.Popen(awk_command,stdout=subprocess.PIPE,shell=True)
    output = p1.stdout.read().strip()
    assert int(output) == int(expected_result)


#3
def test_schema_of_input_file():
    srcVal = get_config("test_schema_of_input_file")
    expected_result = srcVal["expected_schema_sequence"]
    cmd = "grep '%s' %s"%(expected_result,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    expected_country_code_record = p1.stdout.read().strip()
    assert expected_country_code_record.decode("utf-8") == expected_result


#4
def test_column_sequence_of_output_file():
    src_val = get_config("test_column_sequence_of_output_file")
    result = src_val["expected_result"]
    expected_result = ast.literal_eval(result)
    output_file = open(output_path)
    reader = csv.reader(output_file)
    #actual_header = reader.next()
    actual_header = next(reader,None)
    assert expected_result == actual_header


#5
def test_row_count_in_output_csv_after_filtering_and_aggregation():
    src_val = get_config("test_row_count_in_output_csv_after_filtering_and_aggregation")
    expected_count = int(src_val["expected_result_in_output"])
    cmd = "wc -l %s"%(output_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    output = p1.stdout.read().strip()
    output = output.split()
    actual_count = int(output[0])
    assert expected_count == actual_count


#6
def test_min_bid_count_value_for_country_having_30k_bid_request_limit():
    src_val = get_config("test_min_bid_count_value_for_country_having_30k_bid_request_limit")
    publisher_id = src_val["publisher_id"]
    gctry = src_val["gctry"]
    expected_min_bid_count_check = src_val["expected_min_bid_count_check"]
    output_data = pd.read_csv(output_path)
    data = output_data[(output_data.publisher_id == int(publisher_id)) & (output_data.gctry == int(gctry))]
    actual_result =  data['min_bid_count_check'].to_string(index=False,header=False)
    assert int(expected_min_bid_count_check) == int(actual_result)


#7
def test_min_bid_count_value_for_country_having_100k_bid_request_limit():
    src_val = get_config("test_min_bid_count_value_for_country_having_100k_bid_request_limit")
    publisher_id = src_val["publisher_id"]
    gctry = src_val["gctry"]
    expected_min_bid_count_check = src_val["expected_min_bid_count_check"]
    output_data = pd.read_csv(output_path)
    data = output_data[(output_data.publisher_id == int(publisher_id)) & (output_data.gctry == int(gctry))]
    actual_result =  data['min_bid_count_check'].to_string(index=False,header=False)
    assert int(expected_min_bid_count_check) == int(actual_result)


#8
def test_min_bid_count_value_for_country_having_10k_bid_request_limit():
    src_val = get_config("test_min_bid_count_value_for_country_having_10k_bid_request_limit")
    publisher_id = src_val["publisher_id"]
    gctry = src_val["gctry"]
    expected_min_bid_count_check = src_val["expected_min_bid_count_check"]
    output_data = pd.read_csv(output_path)
    data = output_data[(output_data.publisher_id == int(publisher_id)) & (output_data.gctry == int(gctry))]
    actual_result =  data['min_bid_count_check'].to_string(index=False,header=False)
    assert int(expected_min_bid_count_check) == int(actual_result)


#9
def test_invalid_dc_string_filteration():
    src_val = get_config("test_invalid_dc_string_filteration")
    input_data = src_val["input_data"]
    dc = ""
    cmd = "grep '%s' %s"%(input_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    expected_data = p1.stdout.read().strip()
    output_data = pd.read_csv(output_path)
    for index,row in output_data.iterrows():
        assert row["gctry"] != dc

#10
def test_invalid_country_code_filtering():
    src_val = get_config("test_invalid_country_code_filtering")
    gctry_val = src_val["gctry_val"]
    input_data = src_val["input_data"]
    cmd = "grep '%s' %s"%(input_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    expected_data = p1.stdout.read().strip()
    #assert input_data == expected_data
    output_data = pd.read_csv(output_path)
    for index,row in output_data.iterrows():
        assert row["gctry"] != gctry_val


#11
def test_null_country_code_filtering():
    src_val = get_config("test_null_country_code_filtering")
    input_data = src_val["input_data"]
    gctry_val = src_val["gctry_val"]
    cmd = "grep '%s' %s"%(input_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    expected_data = p1.stdout.read().strip()
    assert input_data == expected_data.decode("utf-8")
    output_data = pd.read_csv(output_path)
    for index,row in output_data.iterrows():
        assert row["gctry"] != gctry_val


#12
def test_country_code_less_than_three_filtering():
    src_val = get_config("test_country_code_less_than_three_filtering")
    input_data = src_val["input_data"]
    expected_result = int(src_val["expected_result"])
    cmd = "grep '%s' %s"%(input_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    expected_data = p1.stdout.read().strip()
    assert input_data == expected_data.decode("utf-8")
    output_data = src_val["output_data"]
    cmd = "grep '%s' %s"%(output_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    actual_data = p1.stdout.read().strip()
    assert len(actual_data) == expected_result

#13
def test_cookied_minus_9_filtering_logic():
    src_val = get_config("test_cookied_minus_9_filtering_logic")
    input_data = src_val["input_data"]
    expected_result = int(src_val["expected_result"])
    cmd = "grep '%s' %s"%(input_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    expected_data = p1.stdout.read().strip()
    assert input_data == expected_data.decode("utf-8")
    output_data = src_val["output_data"]
    cmd = "grep '%s' %s"%(output_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    actual_data = p1.stdout.read().strip()
    assert len(actual_data) == expected_result


#14
def test_10K_country_level_bid_request_limit_filtering():
    src_val = get_config("test_10K_country_level_bid_request_limit_filtering")
    input_data = src_val["input_data"]
    expected_result = int(src_val["expected_result"])
    cmd = "grep '%s' %s"%(input_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    expected_data = p1.stdout.read().strip()
    assert input_data == expected_data.decode("utf-8")
    output_data = src_val["output_data"]
    cmd = "grep '%s' %s"%(output_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    actual_data = p1.stdout.read().strip()
    assert len(actual_data) == expected_result


#15
def test_30K_country_level_bid_request_limit_filtering():
    src_val = get_config("test_30K_country_level_bid_request_limit_filtering")
    input_data = src_val["input_data"]
    expected_result = int(src_val["expected_result"])
    cmd = "grep '%s' %s"%(input_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    expected_data = p1.stdout.read().strip()
    assert input_data == expected_data.decode("utf-8")
    output_data = src_val["output_data"]
    cmd = "grep '%s' %s"%(output_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    actual_data = p1.stdout.read().strip()
    assert len(actual_data) == expected_result


#16
def test_100K_country_level_bid_request_limit_filtering():
    src_val = get_config("test_100K_country_level_bid_request_limit_filtering")
    input_data = src_val["input_data"]
    expected_result = int(src_val["expected_result"])
    cmd = "grep '%s' %s"%(input_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    expected_data = p1.stdout.read().strip()
    assert input_data == expected_data.decode("utf-8")
    output_data = src_val["output_data"]
    cmd = "grep '%s' %s"%(output_data,input_path)
    p1 = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    actual_data = p1.stdout.read().strip()
    assert len(actual_data) == expected_result


#17
def test_dc_cluster_to_dc_transformation():
    src_val = get_config("test_dc_cluster_to_dc_transformation")
    publisher_id = src_val["publisher_id"]
    sid = src_val["sid"]
    expected_dc = src_val["expected_dc"]
    campaignid = src_val["campaignid"]
    gctry = src_val["gctry"]
    isCookied = src_val["iscookied"]
    adsize = src_val["adsize"]
    expected_revenue = src_val["expected_revenue"]
    output_data = pd.read_csv(output_path)
    data = output_data[(output_data.publisher_id == int(publisher_id)) & (output_data.sid == int(sid)) & (output_data.campaignid == int(campaignid)) & (output_data.gctry == int(gctry)) & (output_data.isCookied == int(isCookied)) & (output_data.adsize == int(adsize))]
    actual_dc =  data['dc'].to_string(index=False,header=False)
    assert expected_dc == actual_dc.strip()


#18
def test_spend_aggregation():
    src_val = get_config("test_spend_aggregation")
    publisher_id = src_val["publisher_id"]
    sid = src_val["sid"]
    dc = src_val["dc"]
    campaignid = src_val["campaignid"]
    gctry = src_val["gctry"]
    isCookied = src_val["iscookied"]
    adsize = src_val["adsize"]
    expected_revenue = src_val["expected_spend"]
    output_data = pd.read_csv(output_path)
    data = output_data[(output_data.publisher_id == int(publisher_id)) & (output_data.sid == int(sid)) & (output_data.dc == dc) & (output_data.campaignid == int(campaignid)) & (output_data.gctry == int(gctry)) & (output_data.isCookied == int(isCookied)) & (output_data.adsize == int(adsize))]
    actual_result =  data['spend'].to_string(index=False,header=False)
    assert float(expected_revenue) == float(actual_result)


#19
def test_sum_of_uniquebidcnt():
    src_val = get_config("test_sum_of_uniquebidcnt")
    publisher_id = src_val["publisher_id"]
    sid = src_val["sid"]
    dc = src_val["dc"]
    campaignid = src_val["campaignid"]
    gctry =src_val["gctry"]
    isCookied = src_val["iscookied"]
    adsize = src_val["adsize"]
    expected_uniquebidcnt = src_val["expected_uniquebidcnt"]
    output_data = pd.read_csv(output_path)
    data = output_data[(output_data.publisher_id == int(publisher_id)) & (output_data.sid == int(sid)) & (output_data.dc == dc) & (output_data.campaignid == int(campaignid)) & (output_data.gctry == int(gctry)) & (output_data.isCookied == int(isCookied)) & (output_data.adsize == int(adsize))]
    actual_result =  data['uniquebidcnt'].to_string(index=False,header=False)
    assert expected_uniquebidcnt == actual_result.strip()

