# verifying Business logic for column DuringPeriodDiscountAppliedAfterCancellationRenewal

import pytest
import sys
import pandas as pd
sys.path.append('../resource')
from MSSqlDb import MSSqlDbWrapper



def pytest_generate_tests(metafunc):
    argnames = ['plan']
    argvalues = []
    mssql_instance = MSSqlDbWrapper("../config/config1.txt")
    pytest_data = mssql_instance.get_data("SELECT Id, ComputedStartDate, ComputedEndDate  from [Intermediate].[Run]")
    for row in pytest_data:
        _id = str(row[0])
        startdate = row[1]
        enddate = row[2]
        con = mssql_instance.get_connect()
        T1_data = pd.read_sql("SELECT RunId, UserId, UserPlanId, PlanCancelledDate, PlanActionFinal from [Intermediate].[StripeETL] where IgnoreFlag = 1",con)
        T1_data = T1_data[T1_data.RunId == _id]
        T1_data = T1_data[T1_data.PlanCancelledDate != None]
        T1_data = T1_data[(T1_data.PlanCancelledDate>= startdate) & (T1_data.PlanCancelledDate<= enddate)]
        up_data = pd.read_sql("SELECT userId, userPlanId, createdAt, TrialEndsAt from [Intermediate].[UserPlan]",con)
        up_data = up_data[(up_data.createdAt >= startdate) & (up_data.createdAt <= enddate)]
        tt_data = pd.read_sql("SELECT  userId from [Intermediate].[StripeETL] group by userId having count(userId) >1",con)
        #up_data = up_data[up_data.PlatformName == 'Stripe']
        T1_data = T1_data[~T1_data.UserId.isin(list(tt_data['userId']))]              
        
        result = pd.merge(T1_data, up_data,left_on=["UserId","UserPlanId"], right_on=["userId","userPlanId"], how='inner')
        result = result[result.PlanCancelledDate > result.TrialEndsAt]
        print(result)
        for index, row in result.iterrows():
            argvalues.append([row['PlanActionFinal']])
        if len(argvalues) > 20:
            break
    metafunc.parametrize(argnames, argvalues, scope="class")

class TestLeftSWriteColumn:

    def test_valid_left_write(self,plan):
        assert str(plan) == 'Churn'

