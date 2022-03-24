from pyspark import SparkContext, SparkConf
import json
import time
from operator import add
import sys

def getAverageStars(id_stars_rdd,id_city_rdd, option):
    start_time = time.time()
    rddfile = id_city_rdd.join(id_stars_rdd)
    city_stars_rdd = rddfile.map(lambda x: (x[1][0], (x[1][1], 1)))
    if(option == 'm1'):
        result = city_stars_rdd.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
        result = result.map(lambda x: (x[0], x[1][0]/x[1][1])).collect()
        result = sorted(result, key=lambda x:(-x[1],x[0]))
     
    elif(option == 'm2'):
        result = city_stars_rdd.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
        result = result.map(lambda x: (x[0], x[1][0]/x[1][1])).sortBy(lambda x : (-x[1], x[0])).collect()
    
    end_time = time.time()
    exe_time = end_time - start_time
    return result, exe_time

review_filepath = sys.argv[1]
business_filepath = sys.argv[2]
output_filepath1 = sys.argv[3]
output_filepath2 = sys.argv[4]


conf = SparkConf().setMaster('local').setAppName('task3')
sc = SparkContext(conf = conf)

reviewFile = sc.textFile(review_filepath).map(lambda x : json.loads(x))
businessFile = sc.textFile(business_filepath).map(lambda x : json.loads(x))

id_stars_rdd = reviewFile.map(lambda x: (x["business_id"], x['stars']))
id_city_rdd = businessFile.map(lambda x: (x['business_id'], x['city']))


result, exe_time1 = getAverageStars(id_stars_rdd,id_city_rdd, 'm1')
_, exe_time2 =  getAverageStars(id_stars_rdd,id_city_rdd, 'm2')

result2 = {}
result2['m1'] = exe_time1
result2['m2'] = exe_time2
result2['reason'] = "abc"

with open(output_filepath1 , 'w') as outFile:
        outFile.write('city,stars' + '\n')
        for res in result:
            outFile.write(res[0] + ','+ str(res[1])+ '\n')

with open(output_filepath2, "w") as outFile :
    json.dump(result2, outFile)

