import sys

sys.path.append('./resource')
sys.path.append('./experiement')

from TestScript import TestScriptWrapper
from Bid_throttler import Bid_throttler

if len(sys.argv) < 2:
   print("usage is")
   print ("python " + sys.argv[0] + " config_file_path")
   

experiment = Bid_throttler(sys.argv[1])
experiment.run_experiment()

