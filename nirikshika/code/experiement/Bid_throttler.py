from MySqlDb import MysqlDbWrapper
from TestScript import TestScriptWrapper
from HadoopCluster import HadoopClusterWrapper
import subprocess
from datetime import datetime, timedelta
import pandas as pd
import os

import sys
sys.path.append('/opt/Utils/Modules/')
import configReader_python3 as configReader

def get_config(sectionName):
    config = configReader.configReader()
    config_path = "/opt/QA/ML/regular_throttling/regular_bid_throttle_config.ini"
    srcVal = config.getSectionDict(config_path,sectionName)
    return srcVal




class Bid_throttler(object):
    test_script = None
    hadoop_cluster = None

    def __init__(self, configpath):
        self.test_script = TestScriptWrapper(configpath)
        self.hadoop_cluster = HadoopClusterWrapper(configpath)

    def get_day_month_year(self, delta = 0):
        d = datetime.now()-timedelta(days=delta)
        return (d.strftime("%d"), d.strftime("%m"), d.strftime("%Y"))

    def get_weekday(self):
        d = datetime.now()
        return d.weekday()


    def create_input_data_folder(self):
        """This function returns the two paths, where we can put our test data"""
        weekday = self.get_weekday()
    #In java, monday = "2" and in python for monday = "0"
        if weekday == 0:
            day_1, month_1, year_1 = self.get_day_month_year(2)
            day_2, month_2, year_2 = self.get_day_month_year(3)
            input_path1 = "/ml/agg_recon_dailyAgg/"+str(year_1)+"_"+str(month_1)+"_"+str(day_1)
            input_path2 = "/ml/agg_recon_dailyAgg/"+str(year_2)+"_"+str(month_2)+"_"+str(day_2)
            self.hadoop_cluster.delete_file(input_path1)
            self.hadoop_cluster.delete_file(input_path2)
            self.hadoop_cluster.make_dir(input_path1)
            self.hadoop_cluster.make_dir(input_path2)
        else:
            day_1, month_1, year_1 = self.get_day_month_year(1)
            day_2, month_2, year_2 = self.get_day_month_year(2)
            input_path1 = "/ml/agg_recon_dailyAgg/"+str(year_1)+"_"+str(month_1)+"_"+str(day_1)
            input_path2 = "/ml/agg_recon_dailyAgg/"+str(year_2)+"_"+str(month_2)+"_"+str(day_2)
            self.hadoop_cluster.delete_file(input_path1)
            self.hadoop_cluster.delete_file(input_path2)
            self.hadoop_cluster.make_dir(input_path1)
            self.hadoop_cluster.make_dir(input_path2)
        return input_path1,input_path2



    def put_input_data(self):
        """This function puts test data into HDFS"""
        src_val = get_config("ML_path")
        input_data_path = src_val["input_data_path"]
        get_input = self.create_input_data_folder()
        self.hadoop_cluster.put_file(str(input_data_path)+"/part-00008",str(get_input[0])+"/")
        self.hadoop_cluster.put_file(str(input_data_path)+"/part-00009",str(get_input[1])+"/")

    def run_create_daily_data_script(self):
        """This function executes create_daily_data.py script"""
        weekday = self.get_weekday()
        if weekday == 0:
            get_path = get_config("project_home")
            project_path_2 = get_path["project_path_2"]
            command = r"""/usr/local/anaconda3/bin/python %s"""%(project_path_2)
            os.system(command)
            project_path_3 = get_path["project_path_3"]
            command = r"""/usr/local/anaconda3/bin/python %s"""%(project_path_3)
            os.system(command)
        else:
            get_path = get_config("project_home")
            project_path_1 = get_path["project_path_1"]
            command = r"""/usr/local/anaconda3/bin/python %s"""%(project_path_1)
            os.system(command)
            project_path_2 = get_path["project_path_2"]
            command = r"""/usr/local/anaconda3/bin/python %s"""%(project_path_2)
            os.system(command)

     
    def get_input_file_path(self):
        """This function returns two absolute input file path which is present in following path,
          /ML/Deployment/Product/BidThrottling2/Create_data/daily/"""
        weekday = self.get_weekday()
         #In java, monday = "2" and in python for monday = "0"
        if weekday == 0:
            day_1, month_1, year_1 = self.get_day_month_year(2)
            day_2, month_2, year_2 = self.get_day_month_year(3)
            input_path1 = "/ML/Deployment/Product/BidThrottling2/Create_data/daily/"+str(year_1)+"_"+str(month_1)+"_"+str(day_1)+".raw"
            input_path1 = "/ML/Deployment/Product/BidThrottling2/Create_data/daily/"+str(year_1)+"_"+str(month_1)+"_"+str(day_1)+".raw"
            input_path2 = "/ML/Deployment/Product/BidThrottling2/Create_data/daily/"+str(year_2)+"_"+str(month_2)+"_"+str(day_2)+".raw"
        else:
            day_1, month_1, year_1 = self.get_day_month_year(1)
            day_2, month_2, year_2 = self.get_day_month_year(2)
            input_path1 = "/ML/Deployment/Product/BidThrottling2/Create_data/daily/"+str(year_1)+"_"+str(month_1)+"_"+str(day_1)+".raw"
            input_path2 = "/ML/Deployment/Product/BidThrottling2/Create_data/daily/"+str(year_2)+"_"+str(month_2)+"_"+str(day_2)+".raw"
        return input_path1, input_path2


    def merge_two_files(self):
        """This function merged two binary files into one csv file"""
        test_path = get_config("input_output_path")
        input_path = test_path["input_path"]

        input_file = open(input_path, "r+")
        input_file.truncate()
        file_path = self.get_input_file_path()
        raw_to_csv_file_1 = file_path[0]
        raw_to_csv_file_2 = file_path[1]
        df_1 = pd.read_feather(raw_to_csv_file_1)
        df_2 = pd.read_feather(raw_to_csv_file_2)
        frames = [df_1, df_2]
        result = pd.concat(frames)
        result.to_csv(input_path, index = False)

            

    def run_experiment(self):
        self.put_input_data()
        self.run_create_daily_data_script()
        self.test_script.run_python()
        self.merge_two_files()


	
