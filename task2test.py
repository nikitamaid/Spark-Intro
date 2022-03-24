from pyspark import SparkContext, SparkConf
import json
import time
from operator import add
import sys

def funchash(key):
    return hash(key)


def topBusiness(rddfile):
    num = 10
    r = rddfile.reduceByKey(add).takeOrdered(num , key = lambda x: (-x[1], x[0]))

def partitionFunc(ParType):
    result = dict()
    start_time = time.time()
    rddfile = sc.textFile(input_filepath).map(lambda x : json.loads(x))
    rddfile_business = rddfile.map(lambda x : (x['business_id'],1))
    if ParType == 'default':
        topBusiness(rddfile_business)

        
    elif ParType == 'custom':
        rddfile_business = rddfile_business.partitionBy(int(n_partition),funchash)
        topBusiness(rddfile_business)
        
    end_time = time.time()    
    result['n_partition'] = rddfile_business.getNumPartitions()
    result['n_items'] = rddfile_business.glom().map(len).collect()
    result['exe_time'] = end_time - start_time
    return result

input_filepath = sys.argv[1]
output_filepath = sys.argv[2]
n_partition = sys.argv[3]

conf = SparkConf().setMaster('local').setAppName('task2')
sc = SparkContext(conf = conf)
sc.setLogLevel('WARN')


resultDict = dict()
resultDict["default"] = partitionFunc('default')
resultDict["customized"] = partitionFunc('custom')


with open(output_filepath, "w") as outFile :
    json.dump(resultDict, outFile)


