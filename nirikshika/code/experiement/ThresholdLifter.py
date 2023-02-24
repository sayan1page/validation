from MySqlDb import MysqlDbWrapper
from TestScript import TestScriptWrapper

class ThresholdLifter(object):
    mysql_instance = None
    test_script = None
    table_name = None
    column_name = None
    threshold = None

    def __init__(self, configpath):
        self.mysql_instance = MysqlDbWrapper(configpath)
        self.test_script = TestScriptWrapper(configpath)
        f = open(configpath)
        config = {}
        for line in f:
            fields = line.strip().split('=')
            config[fields[0]] = fields[1].strip()
        f.close()
        self.table_name = config['threshold_table_name']
        self.column_name = config['threshold_column_name']
        self.threshold = config['threshold']


    def run_experiment(self):
        self.set_threshold()
        self.test_script.run_python()
        self.mysql_instance.restore_table(self.table_name)
        self.test_script.save_mysql()
	
    def set_threshold(self):
        self.mysql_instance.backup_table(self.table_name)
        self.mysql_instance.set_column(self.table_name,self.column_name,self.threshold)

