#wrappper Script
source venv/bin/activate

#echo "Running MAP2 job without any experiment"
#python Map2or4.py ../config/map2.config

echo "Running job without any experiment"
python BidThrottler.py ../config/bidthrottle.config 
cd analyzer
pytest -v Bid_throttler.py
pytest -v Bid_throttler_organized.py
cd ..
