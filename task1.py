
import sys
from pyspark import SparkContext, SparkConf
import json
from datetime import datetime

def reviewCount(rddfile):
    return rddfile.count()

def reviewCount_year(rddfile,year):
    return rddfile.filter(lambda x : datetime.strptime(x[1], '%Y-%m-%d %H:%M:%S').year == year).count()

def uniqueUser(rddfile):
    return rddfile.distinct().count()

def topUsers(rddfile , num):
    return rddfile.map(lambda x: (x,1)).reduceByKey(add).takeOrdered(num , key = lambda x: (-x[1], x[0]))

def uniqueBusiness(rddfile):
    return rddfile.distinct().count()

def topBusiness(rddfile , num):
    return rddfile.map(lambda x: (x,1)).reduceByKey(add).takeOrdered(num , key = lambda x: (-x[1], x[0]))

input_filepath = sys.argv[1]
output_filepath = sys.argv[2]

resultDict = dict()
year = 2018
num = 10
conf = SparkConf().setMaster('local').setAppName('task1')
sc = SparkContext(conf = conf)
rddfile = sc.textFile(input_filepath).map(lambda x : json.loads(x))

rddfile_ids = rddfile.map(lambda x : x['review_id'])
resultDict["n_review"] = reviewCount(rddfile_ids)

rddfile_idDate = rddfile.map(lambda x : (x['review_id'] ,x['date']))
resultDict["n_review_2018"] = reviewCount_year(rddfile_idDate, year)

rddfile_user = rddfile.map(lambda x : x['user_id'])
resultDict["n_user"] = uniqueUser(rddfile_user)

resultDict["top10_user"] = topUsers(rddfile_user, num)

rddfile_business = rddfile.map(lambda x : x['business_id'])
resultDict["n_business"] = uniqueBusiness(rddfile_business)

resultDict["top10_business"] = topBusiness(rddfile_business, num)



# print(resultDict)

with open(output_filepath, "w") as outFile :
    json.dump(resultDict, outFile)

