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
    if ParType == 'default':
        start_time = time.time()
        rddfile = sc.textFile('test_review.json').map(lambda x : json.loads(x))
        rddfile_business = rddfile.map(lambda x : (x['business_id'],1))
        rddfile = rddfile_business.partitionBy(int(n_partition))
        result['n_partition'] = rddfile_business.getNumPartitions()
        result['n_items'] = rddfile_business.glom().map(len).collect()
        topBusiness(rddfile_business)
        result['exe_time'] = time.time() - start_time
        return result
    elif ParType == 'custom':
        start_time1 = time.time()
        rddfile = sc.textFile('test_review.json').map(lambda x : json.loads(x))
        rddfile_business = rddfile.map(lambda x : (x['business_id'],1))
        rddfile = rddfile_business.partitionBy(int(n_partition),funchash)
        result['n_partition'] = rddfile_business.getNumPartitions()
        result['n_items'] = rddfile_business.glom().map(len).collect()
        r = topBusiness(rddfile_business)
        result['exe_time'] = time.time() - start_time1
        return result

input_filepath = sys.argv[1]
output_filepath = sys.argv[2]
n_partition = sys.argv[3]

conf = SparkConf().setMaster('local').setAppName('task2')
sc = SparkContext(conf = conf)


resultDict = dict()
resultDict["default"] = partitionFunc('default')
resultDict["customized"] = partitionFunc('custom')


with open('output2.json', "w") as outFile :
    json.dump(resultDict, outFile)


