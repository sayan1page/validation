from flask import Flask
from flask_cors import CORS
import sys

sys.path.append('../resource')
from MSSqlDb import MSSqlDbWrapper

app = Flask(__name__)
CORS(app)

@app.route('/demo/hist', methods=['GET','OPTIONS'])
def predict_demo():
	mysql_instance = MSSqlDbWrapper("../config/config1.txt")
	demo_feature = mysql_instance.get_data("select UserPlanId,COUNT(*) as freq from Intermediate.StripeDataETL group by UserPlanId order by freq")		
	data =[['planid','frequency']]
	for row in demo_feature:
		data.append([str(row[0]),row[1]])
	mysql_instance.close_mysql()
	return(str(data))

@app.route('/demo/hist1', methods=['GET','OPTIONS'])
def predict_demo1():
	mysql_instance = MSSqlDbWrapper("../config/config1.txt")
	demo_feature = mysql_instance.get_data("select UserPlanId,COUNT(*) as freq from Intermediate.StripeDataETL group by UserPlanId order by freq")
	data =[["frequency","planid"]]		
	for row in demo_feature:
		data.append([str(row[0]),row[1]])
	mysql_instance.close_mysql()
	return(str(data))


app.run(debug = True)


