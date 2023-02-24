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
    pytest_data = mssql_instance.get_data("SELECT distinct(UserId)  from [Weekly].[Intermediate]")
    for row in pytest_data:
        userId = str(row[0])
        #if int(userId) not in [484414,665716]:
         #   continue
        pytest_data0 = mssql_instance.get_data("SELECT distinct(UserPlanId)  from [Weekly].[Intermediate] where UserId="+userId)
        for row0 in pytest_data0:
            userPlanId = str(row0[0])
            con = mssql_instance.get_connect()
            tt_data = pd.read_sql("SELECT [WeeklyRunId],[MediaTimeInSecsW0],[MediaTimeInSecsW1],[MediaTimeInSecsW2],[MediaTimeInSecsW3],[MediaTimeInSecsW4],[MediaTimeInSecsW5],[MediaTimeInSecsW6],[MediaTimeInSecsW7],[MediaTimeRMean1],[MediaTimeRMean2],[MediaTimeRMean3] ,[MediaTimeRMean4],[MediaTimeRMean5],[MediaTimeRMedian1],[MediaTimeRMedian2],[MediaTimeRMedian3],[MediaTimeRMedian4],[MediaTimeRMedian5],[MediaTimeRSTD1],[MediaTimeRSTD2],[MediaTimeRSTD3],[MediaTimeRSTD4],[MediaTimeRSTD5],[MediaTimeRMin1],[MediaTimeRMin2],[MediaTimeRMin3],[MediaTimeRMin4],[MediaTimeRMin5],[MediaTimeRMax1],[MediaTimeRMax2],[MediaTimeRMax3],[MediaTimeRMax4],[MediaTimeRMax5] FROM [Weekly].[Intermediate] where UserId = "+ str(userId) +" and UserPlanId = "+ str(userPlanId) +" order by WeeklyRunId", con)
            print(tt_data)
            for index1,row1 in tt_data.iterrows():
                print(row1)
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
                median1 = row1['MediaTimeRMedian1']
                median2 = row1['MediaTimeRMedian2']
                median3 = row1['MediaTimeRMedian3']
                median4 = row1['MediaTimeRMedian4']
                median5 = row1['MediaTimeRMedian5']
                std1 = row1['MediaTimeRSTD1']
                std2 = row1['MediaTimeRSTD2']
                std3 = row1['MediaTimeRSTD3']
                std4 = row1['MediaTimeRSTD4']
                std5 = row1['MediaTimeRSTD5']
                min1 = row1['MediaTimeRMin1']
                min2 = row1['MediaTimeRMin2']
                min3 = row1['MediaTimeRMin3']
                min4 = row1['MediaTimeRMin4']
                min5 = row1['MediaTimeRMin5']
                max1 = row1['MediaTimeRMax1']
                max2 = row1['MediaTimeRMax2']
                max3 = row1['MediaTimeRMax3']
                max4 = row1['MediaTimeRMax4']
                max5 = row1['MediaTimeRMax5']
                argvalues.append([mean1, math.floor(st.mean([t0, t1, t2, t3]))])
                idlist.append(userId+"-"+str(_id)+"-MEAN1")
                argvalues.append([mean2, math.floor(st.mean([t1, t2, t3,t4]))])
                idlist.append(userId+"-"+str(_id)+"-MEAN2")
                argvalues.append([mean3,math.floor(st.mean([t2, t3, t4, t5]))])
                idlist.append(userId+"-"+str(_id)+"-MEAN3")
                argvalues.append([mean4, math.floor(st.mean([t3,t4, t5, t6]))])
                idlist.append(userId+"-"+str(_id)+"-MEAN4")
                argvalues.append([mean5, math.floor(st.mean([t4,t5, t6, t7]))])
                idlist.append(userId+"-"+str(_id)+"-MEAN5")
                argvalues.append([median1, st.median([t0, t1, t2, t3])])
                idlist.append(userId+"-"+str(_id)+"-MEDIAN1")
                argvalues.append([median2, st.median([t1, t2, t3,t4])])
                idlist.append(userId+"-"+str(_id)+"-MEDIAN2")
                argvalues.append([median3, st.median([t2, t3, t4, t5])])
                idlist.append(userId+"-"+str(_id)+"-MEDIAN3")
                argvalues.append([median4, st.median([t3, t4, t5, t6])])
                idlist.append(userId+"-"+str(_id)+"-MEDIAN4")
                argvalues.append([median5, st.median([t4, t5, t6, t7])])
                idlist.append(userId+"-"+str(_id)+"-MEDIAN5")
                argvalues.append([std1, round(st.stdev([t0, t1, t2, t3]),4)])
                idlist.append(userId+"-"+str(_id)+"-STD1")
                argvalues.append([std2, round(st.stdev([t1, t2, t3,t4]),4)])
                idlist.append(userId+"-"+str(_id)+"-STD2")
                argvalues.append([std3, round(st.stdev([t2, t3,t4, t5]),4)])
                idlist.append(userId+"-"+str(_id)+"-STD3")
                argvalues.append([std4,round(st.stdev([t3,t4, t5, t6]),4)])
                idlist.append(userId+"-"+str(_id)+"-STD4")
                argvalues.append([std5, round(st.stdev([t4, t5, t6, t7]),4)])
                idlist.append(userId+"-"+str(_id)+"-STD5")
                argvalues.append([min1, min([t0, t1, t2, t3])])
                idlist.append(userId+"-"+str(_id)+"-MIN1")
                argvalues.append([min2, min([t1, t2, t3,t4])])
                idlist.append(userId+"-"+str(_id)+"-MIN2")
                argvalues.append([min3, min([t2, t3, t4, t5])])
                idlist.append(userId+"-"+str(_id)+"-MIN3")
                argvalues.append([min4, min([t3,t4, t5, t6])])
                idlist.append(userId+"-"+str(_id)+"-MIN4")
                argvalues.append([min5, min([t4,t5, t6, t7])])
                idlist.append(userId+"-"+str(_id)+"-MIN5")
                argvalues.append([max1, max([t0, t1, t2, t3])])
                idlist.append(userId+"-"+str(_id)+"-MAX1")
                argvalues.append([max2, max([t1, t2, t3,t4])])
                idlist.append(userId+"-"+str(_id)+"-MAX2")
                argvalues.append([max3, max([t2, t3, t4, t5])])
                idlist.append(userId+"-"+str(_id)+"-MAX3")
                argvalues.append([max4, max([t3,t4, t5, t6])])
                idlist.append(userId+"-"+str(_id)+"-MAX4")
                argvalues.append([max5, max([t4,t5, t6, t7])])
                idlist.append(userId+"-"+str(_id)+"-MAX5")
        if len(argvalues) > 100:
            break
    metafunc.parametrize(argnames, argvalues, ids= idlist,scope="class")

class TestLeftSWriteColumn:

    def test_valid_left_write(self,left, right):
        assert(left == right)
