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

#no output, print the result instead

inRDD = sc.textFile(inputPath)
inRDD = inRDD.map(lambda l: float(l.split(',')[2]))

topVal = inRDD.top(1)[0] #top returns a list, so we get the first element

print(topVal)