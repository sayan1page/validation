import pyodbc
import pandas as pd
import sqlalchemy as sql
from urllib.parse import quote_plus
from datetime import datetime


class InsertLogValues(object):
    connection = None
    cursor = None
    engine = None

    def __init__(self, configpath):
        f = open(configpath)
        config = {}
        for line in f:
            fields = line.strip().split('=')
            config[fields[0]] = fields[1].strip()
        f.close()
        connection_string1 = "DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={};DATABASE={};UID={};PWD={};".format(
            config['mssql_host'], config['mssql_db'], config['mssql_usrname'], config['mssql_passwd'])

        self.connection = pyodbc.connect(connection_string1)
        self.engine = sql.create_engine(
            "mssql+pyodbc://subschurnrd:%s@subschurnpoc.database.windows.net/SubsChurn?driver=SQL+Server+Native+Client+11.0" % quote_plus(config['mssql_passwd']))
        self.cursor = self.connection.cursor()

    def closeConnection(self):
        self.cursor.close()
        self.connection.close()

    def getConnection(self):
        return self.engine

    def insertValues(self, valueList):
        self.getConnection()
        print('Inserting Values...')
        for row in valueList:
            sql = "INSERT INTO Intermediate.AnalysisLogDetail (RunId,ActualValue,TestValue,Relation,OutputTableName,TargetColumnName,Status,StatusReason, LogDateTime,userId,userPlanId) VALUES(?,?,?,?,?,?,?,?,?,?,?)"
            params = (row['RunId'], row['ActualValue'], row['TestValue'],row['relation'], row['outputTableName'],
                      row['targetColumnName'], row['status'], row['statusReason'], row['logDateTime'], row['UserId'], row['UserPlanId'])
            self.cursor.execute(sql, params)
        self.connection.commit()
        self.closeConnection()
        print('Values Inserted Successfully')
