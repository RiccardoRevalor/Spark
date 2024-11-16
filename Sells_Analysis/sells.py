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

def CategoryProductQuantityPrice(l):
    #l = ProductID,ProductName,Category,QuantitySold,Price
    #return (Category, (Product, Quantity, Price))
    fields = l.split(',')
    return (fields[2], (fields[1], fields[3], fields[4]))

inRDD = sc.textFile(inputPath)

header = inRDD.first()

CategoryProductQuantityPriceRDD = inRDD.filter(lambda l: l != header).cache()

#find the total sales for each category
def CategoryQuantityPrice(l):
    #l = ProductID,ProductName,Category,QuantitySold,Price
    #return (Category, (Quantity * Price))
    fields = l.split(',')
    return (fields[2], (float(fields[3]) * float(fields[4])))

CategoryProductQuantityPriceRDD\
.map(CategoryQuantityPrice)\
.reduceByKey(lambda a, b: a + b)\
.map(lambda t: t[0] + '\t' + str(t[1]))\
.saveAsTextFile(outputPath + '/totalSales')

#find the product with the highest price for each category
def CategoryProductPrice(l):
    #return (Category, (Product, Price))
    fields = l.split(',')
    return (fields[2], (fields[1], float(fields[4])))

CategoryProductQuantityPriceRDD\
.map(CategoryProductPrice)\
.reduceByKey(lambda v1, v2: v1 if v1[1] > v2[1] else v2)\
.map(lambda t: t[0] + '\t' + t[1][0] + '\t' + str(t[1][1]))\
.saveAsTextFile(outputPath + '/highestPriceProduct')



spark.stop()
sc.stop()