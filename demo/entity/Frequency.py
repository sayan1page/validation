import sys
sys.path.append('../resource')
from MSSqlDb import MSSqlDbWrapper


class Frequency(object):
	mysql_instance = None
	def __init__(self, configpath="../config/config1.txt"):
				self.mysql_instance = MSSqlDbWrapper(configpath)
		
	def get_histogram(self, table_name="Intermediate.StripeDataETL", column_name="UserPlanId"):
				query = "select " +  column_name + ",COUNT(*) as freq from " + table_name + " group by " + column_name + " order by freq"
				demo_feature = self.mysql_instance.get_data(query)		
				data =[['planid','frequency']]
				for row in demo_feature:
					data.append([str(row[0]),row[1]])
				self.mysql_instance.close_mysql()
				return(str(data))
	
	def get_histogram_reverse(self,table_name="Intermediate.StripeDataETL",column_name="UserPlanId"):
				query = "select " +  column_name + ",COUNT(*) as freq from " + table_name + " group by " +  column_name + " order by freq"
				demo_feature = self.mysql_instance.get_data(query)
				data =[["frequency","planid"]]		
				for row in demo_feature:
					data.append([str(row[0]),row[1]])
				self.mysql_instance.close_mysql()
				return(str(data))


			

	