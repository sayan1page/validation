import os

class HadoopClusterWrapper(object):
    hadoop_bin_path = None
    hdfs_path = None

    def __init__(self, configpath):
        f = open(configpath)
        config = {}
        for line in f:
            fields = line.strip().split('=')
            config[fields[0]] = fields[1].strip()
        f.close()
        self.hadoop_bin_path = config['HADOOP_BIN']


    def put_file(self, src, dest):
        os.system(self.hadoop_bin_path + " fs -put " + src + " " + dest)

    def make_dir(self, dirpath):
        os.system(self.hadoop_bin_path + " fs -mkdir -p " + dirpath)

    def delete_file(self, filepath):
        os.system(self.hadoop_bin_path + "  fs -rm -r " + filepath)

