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

inRDD = sc.textFile(inputPath)
wordsRDD = inRDD.flatMap(lambda l: l.split(' '))

wordCountRDD = wordsRDD.map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y) #create tuples for each word and reduce by key (key=word) to count the number of occurrences

wordCountRDD.sortBy(lambda t: -t[1])\
.map(lambda t: t[0] + '\t' + str(t[1]))\
.saveAsTextFile(outputPath)




spark.stop()
sc.stop()