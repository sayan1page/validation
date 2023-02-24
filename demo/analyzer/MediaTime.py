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
    pytest_data = mssql_instance.get_data("SELECT Id, ComputedStartDate, ComputedEndDate  from [Weekly].[Run]")
    for row in pytest_data:
        _id = str(row[0])
        #if int(_id) not in [112,121,110,108,109]:
         #   continue
        startdate = row[1]
        enddate = row[2]
        print(startdate)
        print(enddate)
        con = mssql_instance.get_connect()
        tt_data = pd.read_sql("SELECT  UserPlanId, MediaTimeInSecsW0 from [Weekly].[Intermediate] where WeeklyRunId = " + str(_id), con)
        print(tt_data)
        for index1, row1 in tt_data.iterrows():
            userPlanId = str(row1['UserPlanId'])
            #if int(userPlanId) not in [198311,198312,98308]:
               # continue
            print(userPlanId)
            T1_data = pd.read_sql("SELECT progress_in_seconds,UserPlanId from [Intermediate].[UserMedia] where UserPlanId=" + str(userPlanId) + " and created_at between '" + str(startdate) + "' and '" + str(enddate) + "'",con)
            T1_data['total'] = T1_data['progress_in_seconds'].sum()
            print(T1_data)
            res = pd.merge(T1_data,tt_data, left_on=["UserPlanId"], right_on=["UserPlanId"])
            print(res)
            for index2, row2 in res.iterrows():
                argvalues.append([row2['total'], row2['MediaTimeInSecsW0']])
                idlist.append(userPlanId+"-"+_id)
                break
    metafunc.parametrize(argnames, argvalues, ids= idlist,scope="class")

class TestLeftSWriteColumn:

    def test_valid_left_write(self,left, right):
        assert(left == right)
