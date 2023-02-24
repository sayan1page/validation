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
    tt_data = pd.read_sql("SELECT  userId from [Intermediate].[StripeETL] group by userId having count(userId) >1",con)
    for index1, row1 in tt_data.iterrows():
        userId = str(row1['userId'])
        if int(userId)  not in [2149759,464462, 514079, 453424, 1228931, 1344368]:
           continue
        print(userId)
        prevPlanCancelledDate = None
        prevPrice = None
        lastplanstatus = None
        T1_data = pd.read_sql("SELECT RunId, ComputeStartDate, ComputeEndDate, UserId, UserPlanId, PlanOriginalStartDate,PlanCancelledDate, PlanActionFinal from [Intermediate].[StripeETL] where IgnoreFlag = 0 and IsCumulativeRecord = 0 and UserId=" + str(userId) + " order by PlanOriginalStartDate",con)
        print(T1_data)
        for index, row in T1_data.iterrows():
            planid = str(row['UserPlanId'])
            planPrice = None
            planTrialDate = None
            isTrial = False
            up_data = pd.read_sql("SELECT createdAt, TrialEndsAt, Price from [Intermediate].[UserPlan] where userId =" + str(userId) + " and userPlanId =" + str(planid),con)
            print(up_data)
            for index2, row2 in up_data.iterrows():
                planTrialDate = row2['TrialEndsAt']
                planPrice = row2['Price']
            if prevPlanCancelledDate == None: 
                if row['PlanCancelledDate'] != None:
                    if (row['PlanCancelledDate'] > row['ComputeStartDate']) and (row['PlanCancelledDate'] < row['ComputeEndDate']):
                        if planTrialDate != None:
                            if row['PlanCancelledDate'] < planTrialDate:
                                isTrial = True
                        else:
                            isTrial = True
                        prevPlanCancelledDate = row['PlanCancelledDate']
                        prevPrice = planPrice
                        lastplanstatus = row['PlanActionFinal']
                        print("Plan cancelled")
                        continue
            if row['PlanOriginalStartDate'] != None and prevPlanCancelledDate != None:
                if (row['PlanOriginalStartDate'] > row['ComputeStartDate']) and (row['PlanOriginalStartDate'] < row['ComputeEndDate']):
                        print("Plan started")
                        if row['PlanOriginalStartDate'] - prevPlanCancelledDate >  datetime.timedelta(1):
                            if not isTrial:
                                argvalues.append([lastplanstatus,'Churn'])
                                idlist.append(userId)
                                print("Churn")
                            else:
                                argvalues.append([lastplanstatus,'NoChange'])
                                idlist.append(userId)
                                print("Churn")
                                prevPrice = planPrice
                                lastplanstatus = row['PlanActionFinal']
                                if prevPlanCancelledDate != row['PlanActionFinal']:
                                    prevPlanCancelledDate = row['PlanActionFinal']
                                    print("Plan cancelled")
                                continue
                        else:
                            if prevPrice > planPrice:
                                argvalues.append([lastplanstatus,'Downgrade'])
                                idlist.append(userId)
                                print("Downgrade")
                            else:
                                if prevPrice < planPrice:
                                    argvalues.append([lastplanstatus,'Upgrade'])
                                    idlist.append(userId)
                                    print("Upgrade")
                                else:
                                    argvalues.append([lastplanstatus,'NoChange'])
                                    idlist.append(userId)
                                    print("Churn")
                            prevPrice = planPrice
                            lastplanstatus = row['PlanActionFinal']
                            if prevPlanCancelledDate != row['PlanActionFinal']:
                                prevPlanCancelledDate = row['PlanActionFinal']
                                print("Plan cancelled")
                            continue
    metafunc.parametrize(argnames, argvalues, ids= idlist,scope="class")

class TestLeftSWriteColumn:

    def test_valid_left_write(self,left, right):
        assert(left == right)


