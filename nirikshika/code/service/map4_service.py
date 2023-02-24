from flask import Flask
from flask_cors import CORS
import sys

sys.path.append('../resource')
from MySqlDb import MysqlDbWrapper

app = Flask(__name__)
CORS(app)


@app.route('/map4/pb_rev', methods=['GET','OPTIONS'])
def predict_pb_rev():
	mysql_instance = MysqlDbWrapper("../../config/map4.config")
	map4_publisher_revenue = mysql_instance.get_data("select PubId, PbRev from map4_final")
	map2_publisher_revenue = mysql_instance.get_data("select PubId, PbRev from map2_final")
	map4_pub_rev_dict = {}
	for row in map4_publisher_revenue:
		map4_pub_rev_dict[row[0]] = row[1]
		
	map2_pub_rev_dict = {}
	for row in map2_publisher_revenue:
		map2_pub_rev_dict[row[0]] = row[1]
		
	data =[['PubId', 'PbRevMap4', 'PbRevMap2']]
	for pubId in map4_pub_rev_dict:
		if pubId in map2_pub_rev_dict:
			data.append([str(pubId), map4_pub_rev_dict[pubId], map2_pub_rev_dict[pubId]])
	mysql_instance.close_mysql()
	return(str(data))
	
@app.route('/map4/pm_rev', methods=['GET','OPTIONS'])
def predict_pm_rev():
	mysql_instance = MysqlDbWrapper("../../config/map4.config")
	map4_publisher_revenue = mysql_instance.get_data("select PubId, PmRev from map4_final")
	map2_publisher_revenue = mysql_instance.get_data("select PubId, PmRev from map2_final")
	map4_pub_rev_dict = {}
	for row in map4_publisher_revenue:
		map4_pub_rev_dict[row[0]] = row[1]
		
	map2_pub_rev_dict = {}
	for row in map2_publisher_revenue:
		map2_pub_rev_dict[row[0]] = row[1]
		
	data =[['PubId', 'PmRevMap4', 'PmRevMap2']]
	for pubId in map4_pub_rev_dict:
		if pubId in map2_pub_rev_dict:
			data.append([str(pubId), map4_pub_rev_dict[pubId], map2_pub_rev_dict[pubId]])
	mysql_instance.close_mysql()
	return(str(data))
	
@app.route('/map4_impact', methods=['GET','OPTIONS'])
def predict_map4_impact():
	mysql_instance = MysqlDbWrapper("../../config/map4.config")
	count_impacted = mysql_instance.get_data("select count(*) from map4_final where Platfee <> TakeRate")
	count_not_impacted = mysql_instance.get_data("select count(*) from map4_final where Platfee = TakeRate")
	data =[['Class', 'Count'],['Impacted', count_impacted[0][0]],['Not Impacted', count_not_impacted[0][0]]]
	mysql_instance.close_mysql()
	return(str(data))
	
@app.route('/map2_impact', methods=['GET','OPTIONS'])
def predict_map2_impact():
	mysql_instance = MysqlDbWrapper("../../config/map4.config")
	count_impacted = mysql_instance.get_data("select count(*) from map2_final where Pfee <> TakeRate")
	count_not_impacted = mysql_instance.get_data("select count(*) from map2_final where Pfee = TakeRate")
	data =[['Class', 'Count'],['Impacted', count_impacted[0][0]],['Not Impacted', count_not_impacted[0][0]]]
	mysql_instance.close_mysql()
	return(str(data))

@app.route('/map4_cross_pf', methods=['GET','OPTIONS'])
def predict_map4_cross_pf():
	mysql_instance = MysqlDbWrapper("../../config/map4.config")
	count_not_exceed = mysql_instance.get_data("select count(*) from map4_final where Platfee >= TakeRate")
	count_exceeded = mysql_instance.get_data("select count(*) from map4_final where Platfee < TakeRate")
	count_zero = mysql_instance.get_data("select count(*) from map4_final where TakeRate=0")
	data =[['Class', 'Count'],['Not Cross', count_not_exceed[0][0]],['Crossed', count_exceeded[0][0]],['Zero', count_zero[0][0]]]
	mysql_instance.close_mysql()
	return(str(data))


@app.route('/map2_cross_pf', methods=['GET','OPTIONS'])
def predict_map2_cross_pf():
	mysql_instance = MysqlDbWrapper("../../config/map4.config")
	count_not_exceed = mysql_instance.get_data("select count(*) from map2_final where Pfee >= TakeRate")
	count_exceeded = mysql_instance.get_data("select count(*) from map2_final where Pfee < TakeRate")
	count_zero = mysql_instance.get_data("select count(*) from map2_final where TakeRate=0")
	data =[['Class', 'Count'],['Not Cross', count_not_exceed[0][0]],['Crossed', count_exceeded[0][0]],['Zero', count_zero[0][0]]]
	mysql_instance.close_mysql()
	return(str(data))


@app.route('/map4_special', methods=['GET','OPTIONS'])
def predict_map4_special():
	mysql_instance = MysqlDbWrapper("../../config/map4.config")
	map4_publisher_take_off = mysql_instance.get_data("select pID,cal_tke_rate from map4_special_result")
		
	data =[['PubId', 'Calculated take off']]
	for row in map4_publisher_take_off:
		data.append([str(row[0]),row[1]])
	
	mysql_instance.close_mysql()
	return(str(data))


app.run(host='0.0.0.0',debug = True)


