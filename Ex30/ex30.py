import findspark
findspark.init()
import pyspark
findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('ex30').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

#input directory
inputPath = './data'

#outut Path
outputPath = './output'

#read the data
logRDD = sc.textFile(inputPath)

#filter the data to get only the lines tha contain the word 'google
resRDD = logRDD.filter(lambda l: 'google' in l)

#store in HDFS 
resRDD.saveAsTextFile(outputPath)