from MySqlDb import MysqlDbWrapper
from TestScript import TestScriptWrapper

class AdjustmentFactor(object):
    komli_mysql_instance = None
    test_script = None
    table_name = None
    column_name = None
    threshold = None

    def __init__(self, configpath):
        self.komli_mysql_instance = MysqlDbWrapper(configpath)
        self.test_script = TestScriptWrapper(configpath)
        f = open(configpath)
        config = {}
        for line in f:
            fields = line.strip().split('=')
            config[fields[0]] = fields[1].strip()
        f.close()
        self.table_name = config['adjustment_factor_table_name']
        self.column_name = config['adjustment_factor_column_name']
        self.threshold = config['adjusted_value']


    def run_experiment(self):
        self.set_adjustment()
        self.test_script.run_python()
        #self.komli_mysql_instance.restore_table(self.table_name)
        self.test_script.save_mysql()
	
    def set_adjustment(self):
        #self.komli_mysql_instance.backup_table(self.table_name)
        self.komli_mysql_instance.set_column(self.table_name,self.column_name,self.threshold,iskomli=True)

