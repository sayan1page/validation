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
        tt_data = pd.read_sql("SELECT  UserPlanId, CollectionCount50PW0 from [Weekly].[Intermediate] where WeeklyRunId = " + str(_id), con)
        print(tt_data)
        for index1, row1 in tt_data.iterrows():
            userPlanId = str(row1['UserPlanId'])
            #if int(userPlanId) not in [198311,198312,98308]:
               # continue
            print(userPlanId)
            T1_data = pd.read_sql("SELECT progress_in_seconds,collection_id,media_id,UserPlanId from [Intermediate].[UserMedia] where UserPlanId=" + str(userPlanId) + " and created_at between '" + str(startdate) + "' and '" + str(enddate) + "'",con)
            for index2, row2 in T1_data.iterrows():
                #if 'media_id' not in row2:
                    #continue
                mediaId = str(row2['media_id'])
                watch_time = row2['progress_in_seconds']
                print(mediaId)
                if mediaId is not None:
                    tt_data = pd.read_sql("SELECT duration from [SubsTyped].[Media] where id=" + str(mediaId), con)
                    print(tt_data)
                    for index3, row3 in tt_data.iterrows():
                        duration = row3['duration']
                        if duration is None or watch_time is None:
                            break
                        p = watch_time/duration
                        print(p)
                        if p > 0.25 and p < 0.5:
                            print("Match Found")
                            tt_data1 = pd.read_sql("SELECT Count(collection_id) as media_count from [Intermediate].[UserMedia] where media_id=" + str(mediaId) + "and UserPlanId=" +str(userPlanId), con);
                            for index4, row4 in tt_data1.iterrows():
                                print([row4['media_count'], row1['CollectionCount50PW0']])
                                argvalues.append([row4['media_count'], row1['CollectionCount50PW0']])
                                idlist.append(mediaId+"-"+_id + "-" + userPlanId)
            if len(argvalues) > 10:
                break
    metafunc.parametrize(argnames, argvalues, ids= idlist,scope="class")

class TestLeftSWriteColumn:

    def test_valid_left_write(self,left, right):
        assert(left == right)
