import findspark
findspark.init()
import pyspark
findspark.find()


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('ex30').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

inputPath = './input'
outputPath = './output'


InRDD = sc.textFile(inputPath)
header = InRDD.first()

baseRDD = InRDD.filter(lambda l: l != header).cache() #IP_ADDRESS TIMESTAMP URL STATUS_CODE RESPONSE_TIME

def urlStatusCode(l):
    #return (URL, StatusCode)
    fields = l.split(' ')
    return (fields[2], int(fields[3]))

baseRDD.map(urlStatusCode).filter(lambda t: t[1] >= 400)\
.map(lambda t: t[0] +'\t' + str(t[1]))\
.saveAsTextFile(outputPath + '/badRecords')

def urlTime(l):
    #return (URL, ResponseTime)
    fields = l.split(' ')
    return (fields[2], float(fields[4]))

urlTimeRDD = baseRDD.map(urlTime).cache()
#calculate average response time for each URL
urlTimeRDD.mapValues(lambda v: (v, 1))\
.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))\
.map(lambda t: t[0] + '\t' + str(t[1][0] / t[1][1]))\
.saveAsTextFile(outputPath + '/averageResponseTime')


#find the most active IP address
#max è un'action !!
mostActiveIP = baseRDD.map(lambda l: (l.split(' ')[0], 1))\
.reduceByKey(lambda a, b: a + b)\
.max(key=lambda t: t[1]) #.saveAsTextFile(outputPath + '/mostActiveIP')


print("mostActiveIP: ", mostActiveIP)

'''
devi mettere mostActiveIP in un una lista perchè sennò parallelize parallelizza la tupla e crea una RDD con i singoli elementi della tupla (e crea [t[0], t[1]])
'''
sc.parallelize([mostActiveIP])\
.map(lambda t: t[0] + '\t' + str(t[1]))\
.saveAsTextFile(outputPath + '/mostActiveIP')


#find the TOP 3 slowest URLs
top3List = urlTimeRDD.top(3, key=lambda t: t[1])

sc.parallelize(top3List)\
.map(lambda t: t[0] + '\t' + str(t[1]))\
.saveAsTextFile(outputPath + '/top3SlowestURLs')




spark.stop()
sc.stop()