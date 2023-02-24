import sys

sys.path.append('./resource')
sys.path.append('./experiement')

from MySqlDb import MysqlDbWrapper
from TestScript import TestScriptWrapper
from AdjustmentFactor import AdjustmentFactor

if len(sys.argv) < 2:
   print("usage is")
   print ("python " + sys.argv[0] + " config_file_path")
   

experiment = AdjustmentFactor(sys.argv[1])
experiment.run_experiment()

