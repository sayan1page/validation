# verifying Business logic for column DuringPeriodDiscountAppliedAfterCancellationRenewal

import pytest
import sys
import pandas as pd
sys.path.append('../resource')
from MSSqlDb import MSSqlDbWrapper



def pytest_generate_tests(metafunc):
    argnames = ['left','right']
    argvalues = []
    mssql_instance = MSSqlDbWrapper("../config/config1.txt")
    pytest_data = mssql_instance.get_data("SELECT Id, ComputedStartDate, ComputedEndDate  from [Intermediate].[Run]")
    for row in pytest_data:
        _id = str(row[0])
        startdate = row[1]
        enddate = row[2]
        con = mssql_instance.get_connect()
        T1_data = pd.read_sql("SELECT RunId, UserId, DuringPeriodDiscountAppliedAfterCancellationRenewal from [Intermediate].[StripeEtl] WHERE DuringPeriodDiscountAppliedAfterCancellationRenewal = 1 and isCumulativeRecord = 0",con)
        T1_data = T1_data[T1_data.RunId == _id]
        T1_data = T1_data[["DuringPeriodDiscountAppliedAfterCancellationRenewal","UserId"]]
        c_data = pd.read_sql( "SELECT percent_off,coupon_code  from [SubsTyped].[Coupon]", con)
        up_data = pd.read_sql("SELECT userId, userPlanId, createdAt, PlatformName, CouponApplied from [Intermediate].[UserPlan]",con)
        up_data = up_data[(up_data.createdAt >= startdate) & (up_data.createdAt <= enddate)]
        tt_data = pd.read_sql("select ROW_NUMBER() OVER(PARTITION BY userId ORDER BY createdAt asc) AS 'RowNumber',userId, userPlanId  from [Intermediate].[UserPlan] where PlatformName = 'Stripe'",con)
        tt_data = tt_data[tt_data.RowNumber == 1]
        up_data = up_data[up_data.PlatformName == 'Stripe']
        up_data = up_data[up_data.CouponApplied != None]
        c_data = c_data[c_data.percent_off != None]
        intermediate_data = pd.merge(up_data,c_data, left_on=['CouponApplied'],right_on=['coupon_code'], how='inner')
        T2_data = intermediate_data[["userId","userPlanId"]]
        T2_data = T2_data[T2_data.userId.isin(list(tt_data['userId']))]        
        T2_data = T2_data[~T2_data.userPlanId.isin(list(tt_data['userPlanId']))]
        T2_data["userId"] = T2_data["userId"].astype(str)
        T2_data["userPlanId"] = T2_data["userPlanId"].astype(str)
        T2_data["CountPercent"] = T2_data.groupby(["userPlanId"])['userPlanId'].transform('count')        
        T1_data["UserId"] = T1_data["UserId"].astype(str)
        T1_data["UserId"] = pd.to_numeric(T1_data["UserId"])
        T2_data["userId"] = pd.to_numeric(T2_data["userId"])
        result = pd.merge(T1_data, T2_data,left_on=["UserId"], right_on=["userId"], how='inner')
        print(result)
        for index, row in result.iterrows():
            argvalues.append([row['DuringPeriodDiscountAppliedAfterCancellationRenewal'],row['CountPercent']])
        if len(argvalues) > 20:
            break
    metafunc.parametrize(argnames, argvalues, scope="class")

class TestLeftSWriteColumn:

    def test_valid_left_write(self, left, right):
        assert float(left) == float(right)

