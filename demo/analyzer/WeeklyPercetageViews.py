import pytest
import sys
import pandas as pd
import math
sys.path.append('../resource')
from MSSqlDb import MSSqlDbWrapper
import numpy as np
from datetime import datetime, timedelta

class test:

    def is_nan(x):
        return (x != x)

    #  1,5,6,7,8,9,113
    #  Science ,History,Technology,Nature,Society,Lifestyle,Kids
    
    def CountOfCollectionsAccessedDuringPeriodByViewsWeekly(self, td):
        print("------------Test start CountOfCollectionsAccessedDuringPeriodByViewsWeekly--------------------")       

        ## ---------input parameters Start -----
        offset = 200000
        limit = 40000
        #runId = 23
        categoryId = 1
        categoryColumnName = "CollectionCount50PW1"
        minPercentage = 25.00
        maxPercentage = 50.00
  
       

        

        ## ---------input parameters End -----

        mssql_instance = MSSqlDbWrapper("../config/config1.txt")
        pytest_data = mssql_instance.get_data('SELECT Id, ComputedStartDate, ComputedEndDate  from [Weekly].[Run] where Id < 108 ')   
        print("-------------Run Details---------------")     
        print(pytest_data)
        for row in pytest_data:

            _id = str(row[0])
            runId = str(row[0])
            print("RunId - "+ runId)
            startdate1 = str(row[1])
            enddate1 = str(row[2])
            startdate = str(row[1]- timedelta(td))
            enddate = str(row[2]- timedelta(td))
            
            print(startdate)
            print(enddate)
            con = mssql_instance.get_connect()

            etl_query = "SELECT WeeklyRunId,UserId,UserPlanId , "+ categoryColumnName+ " from [Weekly].[Intermediate]  where WeeklyRunId = " +str(runId)+ " ORDER BY userId ASC OFFSET "+ str(offset)+ " rows fetch next " + str(limit)+" rows only"
            etl_data = pd.read_sql(etl_query,con)
            
            etl_data = etl_data[etl_data.WeeklyRunId == row[0]]
           
            um_query = "SELECT  user_id,UserPlanId,progress_in_seconds, collection_id, media_id, created_at from [Intermediate].[UserMedia] where user_id in  ( select userId from [Weekly].[Intermediate]  where WeeklyRunId = " +str(runId)+ "  ORDER BY userId ASC OFFSET "+ str(offset)+ " rows fetch next "+ str(limit)+" rows only )"
            
            um_data = pd.read_sql(um_query,con)


            m_data = pd.read_sql("SELECT  Id , duration from [SubsTyped].[Media]",con)
            

            #joining etl and um
            intermediate_etl_um = pd.merge(etl_data, um_data, left_on=['UserId','UserPlanId'],right_on=['user_id','UserPlanId'], how='left')
            #m.id = um.media_id
            intermediate_etlUm_m = pd.merge(intermediate_etl_um, m_data, left_on=['media_id'],right_on=['Id'], how='left')
            #MC.media_id = UM.media_id
            #print(intermediate_etlUm_m)
            intermediate_etlUm_m = intermediate_etlUm_m[(intermediate_etlUm_m.created_at > startdate )]
            intermediate_etlUm_m = intermediate_etlUm_m[(intermediate_etlUm_m.created_at <= enddate )]
            intermediate_etlUm_m = intermediate_etlUm_m[intermediate_etlUm_m.collection_id.notnull()]
            intermediate_etlUm_m['percentage'] = 0.000

            for index, row in intermediate_etlUm_m.iterrows():
                #print(str((row['progress_in_seconds']* 100) / row['duration']))
                #print(round((row['progress_in_seconds']* 100) / row['duration'])) 
                intermediate_etlUm_m.at[index ,'percentage'] = round((row['progress_in_seconds']* 100 / row['duration'] ),1)
                #print(intermediate_etlUm_m.at[index ,'percentage'])
                #print( intermediate_etlUm_m.at[index ,'percentage'])            
            #intermediate_etlUm_m = intermediate_etlUm_m[intermediate_etlUm_m.collection_id.notnull()]
            intermediate_etlUm_m = intermediate_etlUm_m[((intermediate_etlUm_m.percentage) > (minPercentage)) ]
            intermediate_etlUm_m = intermediate_etlUm_m[((intermediate_etlUm_m.percentage) <= (maxPercentage ))]
            grouped = intermediate_etlUm_m.groupby(['UserId','UserPlanId', categoryColumnName])['collection_id'].count().reset_index(name="collection_count")

            # um.progress_in_seconds * 100.00 / (m.duration)
            #grouped = grouped[[progress_in_seconds] * 100.00 / [duration]].reset_index(name="c_count")
            
            grouped['Result_check'] = np.where((grouped[categoryColumnName] == grouped['collection_count']), True, False)
            #print(grouped)
            print("------------Result -----------------")
            grouped2 = grouped[(grouped.Result_check == False)]
            print (grouped2)
            

            #grouped['statusReason'] = np.where(
                #(grouped[categoryColumnName] == grouped['collection_count']), 'Matched', 'Value Mismatch')
            #grouped['targetColumnName'] = categoryColumnName
            #grouped['outputTableName'] = 'Stripe.ETL'
            #grouped['relation'] = 'Equals'
            #grouped['actualValue'] = grouped[categoryColumnName]
            #grouped['testValue'] = grouped['total_count']
            #grouped['logDateTime'] = datetime.now()
            #print("------------Result -----------------")
            #path = 'C:/Users/SymphonyAI/Desktop/ScriptOutput/config1.txt'
            #a = InsertLogValues(path)
            #a.insertValues(grouped.to_dict('records'))







new_object = test()
new_object.CountOfCollectionsAccessedDuringPeriodByViewsWeekly(7)



