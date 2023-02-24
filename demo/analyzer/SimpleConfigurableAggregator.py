# verifying Business logic for column DuringPeriodDiscountAppliedAfterCancellationRenewal

import pytest
import sys
import pandas as pd
sys.path.append('../resource')
from MSSqlDb import MSSqlDbWrapper
from InsertLogValues import InsertLogValues
from dateutil.relativedelta import relativedelta
from datetime import datetime



def pytest_generate_tests(metafunc):
    argnames = ['left','right']
    argvalues = []
    idlist = []
    config = {}
    f = open('config6_D.txt')
    for line in f:
        fields = line.split('=')
        config[fields[0].strip()] = fields[-1].strip()
    mssql_instance = MSSqlDbWrapper("../config/config1.txt")
    pytest_data = mssql_instance.get_data("SELECT Id, ComputedStartDate, ComputedEndDate  from [Intermediate].[Run]")
    for row in pytest_data:
        _id = str(row[0])
        startdate = row[1]
        enddate = row[2]
        startdate = startdate - relativedelta(months=int(config['Number_of_months']))
        print(startdate)
        con = mssql_instance.get_connect()
        aggregation_query = "SELECT RunId, computeStartDate, " + config["group_by_column"] + "," + config["agregated_column"] + " from " + config["agregated_table"] + " where " + config["filtering_column"] + " = " + config["filter_value"] + " and ignoreFlag = 0"
        print(aggregation_query)
        T1_data = pd.read_sql(aggregation_query,con)
        #T1_data = T1_data[T1_data.RunId == _id]
        T1_data = T1_data[(T1_data.computeStartDate >= startdate) & (T1_data.computeStartDate <= enddate)]
        str1 = config["group_by_column"]
        arr = str1.split(",")
        if config['aggregation_type'] == "sum":
            T1_data["Aggregated"] = T1_data.groupby(arr)[config["agregated_column"]].transform('sum')

        target_query = "SELECT " + config["compared_on_column"] + "," + config["target_column"] + " from " + config["target_table"] + " where " + config["filtering_column"] + " = " + config["filter_value"] + " and ignoreFlag = 0 and RunId = " + _id
        print(target_query)
        T2_data = pd.read_sql(target_query,con)
        result = pd.merge(T1_data, T2_data,left_on=arr, right_on=arr, how='inner')
        print(result)
        valueList = []
        for index, row in result.iterrows():
            argvalues.append([row[config["target_column"]],row["Aggregated"]])
            idlist.append(str(_id) + "-" + str(row['UserId']) +"-" + str(row['UserPlanId']))
            res = {}
            res['ActualValue'] = row[config["target_column"]]
            res['TestValue'] = row["Aggregated"]
            res['relation'] = "Equal" 
            res['outputTableName'] = "AnalysisLogDetail"
            res['targetColumnName']= config["target_column"]
            res['status'] = str(res['ActualValue'] == res['TestValue'])
            res['statusReason'] = "None" 
            res['logDateTime'] = datetime.now()
            res['UserId'] = row['UserId']
            res['UserPlanId'] = row['UserPlanId']
            res['RunId'] = _id
            valueList.append(res)
        if len(argvalues) > 10000:
            break
    a = InsertLogValues("../config/config2.txt")
    a.insertValues(valueList)
    metafunc.parametrize(argnames, argvalues,ids=idlist, scope="class")

class TestLeftSWriteColumn:

    def test_valid_left_write(self,left,right):
        assert left == right

