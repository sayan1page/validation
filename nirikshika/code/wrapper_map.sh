#wrappper Script
source venv/bin/activate

#echo "Running MAP2 job without any experiment"
#python Map2or4.py ../config/map2.config

echo "Running MAP4 job without any experiment"
python Map2or4.py ../config/map4.config
cd analyzer
pytest -v -s PytestVerifier.py
pytest -v -s JsonVerifier.py
cd ..

echo "Running MAP4 job making pfee 100%"
python Map4ThresholdLiftMain.py ../config/map4.config
cd analyzer
pytest -v -s PytestVerifier.py
pytest -v -s JsonVerifier.py
cd ..

echo "Running MAP4 job making adjustment factor 2"
python Map4AdjustmentFactore.py ../config/map4.config
cd analyzer
pytest -v -s PytestVerifier.py
pytest -v -s JsonVerifier.py
cd ..
