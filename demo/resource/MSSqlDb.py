import  pyodbc
#import pandas as pd
import sqlalchemy as sql
from urllib.parse import quote_plus


class MSSqlDbWrapper(object):
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
		connection_string1 = "DRIVER={{SQL Server}};SERVER={};DATABASE={};UID={};PWD={};".format(config['mssql_host'], config['mssql_db'],config['mssql_usrname'], config['mssql_passwd'])
		#connection_string2 = "mssql+pyodbc://"+ config['mssql_usrname'] +":" + config['mssql_passwd']+ "@" + config['mssql_host'] + "/" + config['mssql_db'] + "?driver=SQL+Server+Native+Client"
		#connection_string2 = "mssql+pyodbc://subschurnrd:%s@subschurnpoc.database.windows.net/SubsChurn" % quote_plus(config['mssql_passwd'])

		self.connection = pyodbc.connect (connection_string1)
		self.engine = sql.create_engine ("mssql+pyodbc://subschurnrd:%s@subschurnpoc.database.windows.net/SubsChurn?driver=SQL+Server+Native+Client+11.0" % quote_plus(config['mssql_passwd']))
		self.cursor = self.connection.cursor()

	def close_mysql(self):
		self.cursor.close()
		self.connection.close()

	def get_connect(self):
		return self.engine

	def get_data(self, query):
		self.cursor.execute(query)
		return self.cursor.fetchall()

	def load_df(self, query):
		pass #return pd.read_sql(query, self.dbConnection)

	def set_column(self,tablename, column_name, value):
		sql = "UPDATE " + tablename + " SET "+ column_name +"=" + value 
		self.cursor.execute(sql)
		self.connection.commit()

	def backup_table(self, tablename):
		src = tablename
		dest = tablename + "_back"
		self.copy_table(src, dest)

	def restore_table(self, tablename):
		src = tablename + "_back"
		dest = tablename
		self.copy_table(src, dest)

	def copy_table(self, source, dest):
		self.cursor.execute("DROP TABLE IF EXISTS " + dest)
		self.cursor.execute("create table " + dest + " like " + source)
		self.cursor.execute("insert into " + dest + " select * from " + source)
		self.connection.commit()
	

