# verifying Business logic for column DuringPeriodDiscountAppliedAfterCancellationRenewal

import pytest
import sys
import pandas as pd
import numpy as np
import datetime
import statistics as st
import math

sys.path.append('../resource')
from MSSqlDb import MSSqlDbWrapper

def pytest_generate_tests(metafunc):
    argnames = ['left', 'right']
    argvalues = []
    idlist = []
    mssql_instance = MSSqlDbWrapper("../config/config1.txt")
    con = mssql_instance.get_connect()
    finished = False
    con = mssql_instance.get_connect()
    tt_data = pd.read_sql("SELECT [WeeklyRunId],[UserId],[MediaTimeInSecsW0],[MediaTimeInSecsW1],[MediaTimeInSecsW2],[MediaTimeInSecsW3],[MediaTimeInSecsW4],[MediaTimeInSecsW5],[MediaTimeInSecsW6],[MediaTimeInSecsW7],[MediaTimeRMean1],[MediaTimeRMean2],[MediaTimeRMean3] ,[MediaTimeRMean4],[MediaTimeRMean5] FROM [Weekly].[Intermediate]", con)   
    print(tt_data)
    for index1,row1 in tt_data.iterrows():
        userId = row1['UserId']
        _id = row1['WeeklyRunId']
        t0 = row1['MediaTimeInSecsW0']
        t1 = row1['MediaTimeInSecsW1']
        t2 = row1['MediaTimeInSecsW2']
        t3 = row1['MediaTimeInSecsW3']
        t4 = row1['MediaTimeInSecsW4']
        t5 = row1['MediaTimeInSecsW5']
        t6 = row1['MediaTimeInSecsW6']
        t7 = row1['MediaTimeInSecsW7']
        mean1 = row1['MediaTimeRMean1']
        mean2 = row1['MediaTimeRMean2']
        mean3 = row1['MediaTimeRMean3']
        mean4 = row1['MediaTimeRMean4']
        mean5 = row1['MediaTimeRMean5']
        if all(x==0.0 for x in [t0,t1,t2,t3,t4,t5,t6,t7]):
                print("skipped")
                continue
        test_id = str(userId)+"-"+str(_id)+"-MEAN1"
        if test_id not in idlist:
                argvalues.append([mean1, math.floor(st.mean([t0, t1, t2, t3]))])
                idlist.append (str(userId)+"-"+str(_id)+"-MEAN1")
        test_id = str(userId)+"-"+str(_id)+"-MEAN2"
        if test_id not in idlist:
                argvalues.append([mean2, math.floor(st.mean([t1, t2, t3,t4]))])
                idlist.append (str(userId)+"-"+str(_id)+"-MEAN2")
        test_id = str(userId)+"-"+str(_id)+"-MEAN3"
        if test_id not in idlist:
                argvalues.append([mean3,math.floor(st.mean([t2, t3, t4, t5]))])
                idlist.append (str(userId)+"-"+str(_id)+"-MEAN3")
        test_id = str(userId)+"-"+str(_id)+"-MEAN4"
        if test_id not in idlist:
                argvalues.append([mean4, math.floor(st.mean([t3,t4, t5, t6]))])
                idlist.append(str(userId)+"-"+str(_id)+"-MEAN4")
        test_id = str(userId)+"-"+str(_id)+"-MEAN5"
        if test_id not in idlist:
                argvalues.append([mean5, math.floor(st.mean([t4,t5, t6, t7]))])
                idlist.append(str(userId)+"-"+str(_id)+"-MEAN5")
                
    metafunc.parametrize(argnames, argvalues, ids= idlist,scope="class")

class TestLeftSWriteColumn:

    def test_valid_left_write(self,left, right):
        assert(left == right)
