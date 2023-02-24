# verifying Business logic for column DuringPeriodDiscountAppliedAfterCancellationRenewal

import pytest
import sys
import pandas as pd
import numpy as np
import datetime
sys.path.append('../resource')
from MSSqlDb import MSSqlDbWrapper

def pytest_generate_tests(metafunc):
    argnames = ['value']
    argvalues = []
    idlist = []
    mssql_instance = MSSqlDbWrapper("../config/config1.txt")
    con = mssql_instance.get_connect()
    finished = False
    pytest_data = mssql_instance.get_data("SELECT distinct(UserId)  from [Weekly].[Intermediate]")
    for row in pytest_data:
        userId = str(row[0])
        #if int(userId) not in [484414,665716]:
         #   continue
        pytest_data0 = mssql_instance.get_data("SELECT distinct(UserPlanId)  from [Weekly].[Intermediate] where UserId="+userId)
        for row0 in pytest_data0:
            userPlanId = str(row0[0])
            con = mssql_instance.get_connect()
            tt_data = pd.read_sql("SELECT [UserId], [UserPlanId], [WeeklyRunId], [PlanAction], [ChurnLabel] FROM [Weekly].[Intermediate] where UserId = "+ str(userId) +" and UserPlanId = "+ str(userPlanId) +" order by WeeklyRunId", con)
            print(tt_data)
            for index1,row1 in tt_data.iterrows():
                Label = row1['ChurnLabel']
                if Label:
                    _id = row1['WeeklyRunId']
                    limit_id = _id + 8
                    tt1_data = pd.read_sql("SELECT count(*) as churncount FROM [Weekly].[Intermediate] where PlanAction = 'Churn' and WeeklyRunId <= "+ str(limit_id) +" and UserId = "+ str(userId) +" and UserPlanId = "+ str(userPlanId), con)
                    for index2,row2 in tt1_data.iterrows():
                        value = row2['churncount']
                        argvalues.append([value])
                        idlist.append(str(userId)+"-"+ str(userPlanId)+"-"+str(_id))
        if len(argvalues) > 1000:
            break
    metafunc.parametrize(argnames, argvalues, ids= idlist,scope="class")

class TestLeftSWriteColumn:

    def test_valid_left_write(self,value):
        assert(value > 0)
