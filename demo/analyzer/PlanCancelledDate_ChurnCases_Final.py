# verifying Business logic for column DuringPeriodDiscountAppliedAfterCancellationRenewal

import pytest
import sys
import pandas as pd
import numpy as np
import datetime
sys.path.append('../resource')
from MSSqlDb import MSSqlDbWrapper



def pytest_generate_tests(metafunc):
    argnames = ['plan','EndDate','StartDate','PlanCancelledDate','PlanOriginalStartDate','UserId','UserPlanId']
    argvalues = []
    mssql_instance = MSSqlDbWrapper("../config/config1.txt")
    pytest_data = mssql_instance.get_data("SELECT Id, ComputedStartDate, ComputedEndDate  from [Intermediate].[Run]")
    for row in pytest_data:
        _id = str(row[0])
        startdate = row[1]
        enddate = row[2]
        con = mssql_instance.get_connect()
        T1_data = pd.read_sql("SELECT RunId, UserId, UserPlanId, PlanOriginalStartDate, PlanEndDate, PlanCancelledDate, PlanActionFinal from [Intermediate].[StripeETL] where IgnoreFlag = 0 and IsCumulativeRecord = 0",con)
        T1_data = T1_data[T1_data.RunId == _id]
        tt_data = pd.read_sql("SELECT  userId from [Intermediate].[StripeETL] group by userId having count(userId) >1",con)
        T1_data = T1_data[T1_data.UserId.isin(list(tt_data['userId']))]
        for index, row in T1_data.iterrows():
            argvalues.append([row['PlanActionFinal'],enddate, startdate,row['PlanCancelledDate'],row['PlanOriginalStartDate'],row['UserId'],row['UserPlanId']])
            if len(argvalues) > 500:
                break
        if len(argvalues) > 500:
            break

    metafunc.parametrize(argnames, argvalues, scope="class")

class TestLeftSWriteColumn:

    def test_valid_left_write(self,plan,EndDate,StartDate,PlanCancelledDate, PlanOriginalStartDate, UserId, UserPlanId):
        #if plan == 'NoChange':
         #   assert (PlanOriginalStartDate >= EndDate or PlanOriginalStartDate <= StartDate) or np.isnat(np.datetime64(str(PlanOriginalStartDate)) or np.isnat(np.datetime64(str(PlanCancelledDate))))
          #  assert (PlanCancelledDate >= EndDate or PlanCancelledDate <= StartDate) or np.isnat(np.datetime64(str(PlanCancelledDate)))
        if plan == 'Churn':
            assert(EndDate >= PlanCancelledDate and StartDate <= PlanCancelledDate)
            assert(abs(PlanOriginalStartDate - PlanCancelledDate) >= datetime.timedelta(1))


