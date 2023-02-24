import sys

sys.path.append('./resource')
sys.path.append('./experiement')

from TestScript import TestScriptWrapper
from NoExperiment import NoExperiment

if len(sys.argv) < 2:
   print("usage is")
   print ("python " + sys.argv[0] + " config_file_path")
   

experiment = NoExperiment(sys.argv[1])
experiment.run_experiment()

