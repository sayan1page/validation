# verifying Business logic for column DuringPeriodDiscountAppliedAfterCancellationRenewal

import pytest
import sys
import pandas as pd
import numpy as np
sys.path.append('../resource')
from MSSqlDb import MSSqlDbWrapper
from InsertLogValues import InsertLogValues
from datetime import datetime

def pytest_generate_tests(metafunc):
    argnames = ['UserId', 'planaction','eventdate','startdate','enddate']
    argvalues = []
    idlist = []
    valueList = []
    config = {}
    f = open('configA.txt')
    for line in f:
        fields = line.split('=')
        config[fields[0].strip()] = fields[-1].strip()
    mssql_instance = MSSqlDbWrapper("../config/config1.txt")
    con = mssql_instance.get_connect()
    T1_data = pd.read_sql("SELECT  UserId,PlanAction,EventDate,PlanStartDate,PlanEndDate FROM [Intermediate].[ScaledStripeUserJourney]",con)
    for index, row in T1_data.iterrows():
        argvalues.append([row['UserId'], row['PlanAction'],row['EventDate'],row['PlanStartDate'],row['PlanEndDate']])
        idlist.append(str(row['UserId']))
        res = {}
        res['ActualValue'] = row[config["compare_column"]]
        res['TestValue'] = row[config["target_column"]]
        res['relation'] = ">=" 
        res['outputTableName'] = "AnalysisLogDetail"
        res['targetColumnName']= config['target_column']
        res['status'] = str(res['ActualValue'] <= res['TestValue'])
        res['statusReason'] = "None" 
        res['logDateTime'] = datetime.now()
        res['UserId'] = row[config["refer_column"]]
        res['UserPlanId'] = 0
        res['RunId'] = 0
        valueList.append(res)
        if len(argvalues) > 500:
            break
    a = InsertLogValues("../config/config2.txt")
    a.insertValues(valueList)
    metafunc.parametrize(argnames, argvalues,ids=idlist, scope="class")

class TestLeftSWriteColumn:

    def test_valid_left_write(self,UserId,planaction,eventdate,startdate, enddate):
        assert startdate <= eventdate or np.isnat(np.datetime64(str(startdate)))
        assert enddate >= eventdate or np.isnat(np.datetime64(str(enddate)))
        assert planaction in ["NoChange","Upgrade", "Downgrade", "Churn"]

