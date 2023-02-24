import  MySQLdb
import pandas as pd
from sqlalchemy import create_engine


class MysqlDbWrapper(object):
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
        self.connection = MySQLdb.connect (host = config['mysql_host'], user = config['mysql_usrname'], passwd = config['mysql_passwd'], db = config['mysql_db'])
        self.cursor = self.connection.cursor()
        self.engine = create_engine("mysql+mysqldb://"+config['mysql_usrname']+":"+config['mysql_passwd']+"@"+ config['mysql_host']+"/"+config['mysql_db'])

    def close_mysql(self):
        self.cursor.close()
        self.connection.close()

    def get_data(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def load_df(self, query):
        return pd.read_sql(query, self.connection)

    def store_df(self, df, tablename):
        df.to_sql(con=self.engine, name=tablename, if_exists='replace')

    def set_column(self,tablename, column_name, value,iskomli=False):
        if not iskomli:
            sql = "UPDATE " + tablename + " SET "+ column_name +"=" + value 
            self.cursor.execute(sql)
            self.connection.commit()
        else:
            sql = ""
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
	

