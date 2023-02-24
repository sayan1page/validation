import sys

sys.path.append('./resource')
sys.path.append('./experiement')

from MySqlDb import MysqlDbWrapper
from TestScript import TestScriptWrapper
from ThresholdLifter import ThresholdLifter

if len(sys.argv) < 2:
   print("usage is")
   print ("python " + sys.argv[0] + " config_file_path")
   

experiment = ThresholdLifter(sys.argv[1])
experiment.run_experiment()

