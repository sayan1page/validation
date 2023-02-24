from flask import Flask
from flask_cors import CORS
import sys

sys.path.append('../entity')
from Frequency import Frequency

app = Flask(__name__)
CORS(app)

@app.route('/demo/hist', methods=['GET','OPTIONS'])
def predict_demo():
	freq_instance = Frequency("../config/config1.txt")
	return(freq_instance.get_histogram())

@app.route('/demo/hist1', methods=['GET','OPTIONS'])
def predict_demo1():
	freq_instance = Frequency("../config/config1.txt")
	return(freq_instance.get_histogram_reverse())


app.run(debug = True)


