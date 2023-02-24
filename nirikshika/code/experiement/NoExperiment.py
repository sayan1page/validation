from MySqlDb import MysqlDbWrapper
from TestScript import TestScriptWrapper

class NoExperiment(object):
    test_script = None

    def __init__(self, configpath):
        self.test_script = TestScriptWrapper(configpath)


    def run_experiment(self):
        self.test_script.run_python()
        self.test_script.save_mysql()
	
