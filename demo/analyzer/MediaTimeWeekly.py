# verifying Business logic for column DuringPeriodDiscountAppliedAfterCancellationRenewal

import pytest
import sys
import pandas as pd
import numpy as np
import datetime
sys.path.append('../resource')
from MSSqlDb import MSSqlDbWrapper

def pytest_generate_tests(metafunc):
    argnames = ['left', 'right']
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
            tt_data = pd.read_sql("SELECT [WeeklyRunId],[MediaTimeInSecsW0],[MediaTimeInSecsW1],[MediaTimeInSecsW2],[MediaTimeInSecsW3],[MediaTimeInSecsW4],[MediaTimeInSecsW5],[MediaTimeInSecsW6],[MediaTimeInSecsW7] FROM [Weekly].[Intermediate] where UserId = "+ str(userId) +" and UserPlanId = "+ str(userPlanId) +" order by WeeklyRunId", con)
            print(tt_data)
            left = 0
            _id = -1
            for index1,row1 in tt_data.iterrows():
                print(row1)
                w0 = row1['MediaTimeInSecsW0']
                if w0 > 0:
                    left = w0
                    _id = row1['WeeklyRunId']
                current_id = row1['WeeklyRunId']
                if _id != -1:
                    diff = current_id - _id
                    if diff < 8:
                        cname = "MediaTimeInSecsW"+str(diff)
                        right = row1[cname]
                        print(left)
                        print(right)
                        argvalues.append([left,right])
                        idlist.append(userId+"-"+str(_id))
        if len(argvalues) > 100:
            break
    metafunc.parametrize(argnames, argvalues, ids= idlist,scope="class")

class TestLeftSWriteColumn:

    def test_valid_left_write(self,left, right):
        assert(left == right)
