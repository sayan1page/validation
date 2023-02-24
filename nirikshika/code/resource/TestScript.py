import os
import pandas as pd
from MySqlDb import MysqlDbWrapper

class TestScriptWrapper(object):
    script_path = None
    script_command = None
    python_path= None
    arguments = ""
    output_path=None
    mysql_instance = None
    output_table = None

    def __init__(self, configpath):
        self.mysql_instance =  MysqlDbWrapper(configpath)
        f = open(configpath)
        config = {}
        for line in f:
            fields = line.strip().split('=')
            config[fields[0]] = fields[1].strip()
        f.close()
        self.script_path = config['script_path']
        self.python_path = config['python_path']
        if 'argument' in config:
            self.arguments = config['arguments']
        self.output_path= config['output_file_name']
        self.output_table = config['output_table_name']

    def run_python(self,path=None):
        command = self.python_path+ ' ' +self.script_path+ ' ' + self.arguments + '>' + self.output_path + '\n'
        os.system(command)

    def save_mysql(self):
        df = pd.read_csv(self.output_path) 
        df.columns = df.columns.str.strip()
        self.mysql_instance.store_df(df,self.output_table)


    def run_hadoop(self, path=None):
        pass	

    def run_scla(sef,path=None):
        pass

    def run_R(self,path=None):
        pass

