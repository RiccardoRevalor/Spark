import findspark
findspark.init()
import pyspark
findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('ex30').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

dictFile ='./input/dictionary.txt'
textFile = './input/text.txt'


dictRDD = sc.textFile(dictFile).map(lambda l: (l.split(' ')[0], l.split(' ')[1]))

dict = dictRDD.collectAsMap() #this creates a dictionary from the RDD

dict = sc.broadcast(dict) #broadcast the dictionary to all nodes

textRDD = sc.textFile(textFile)

#map the words of the text and replace them with the dictionary values
def mapWords(line):
    words = line.split(' ')
    for word in words:
        if word in dict.value:
            line = line.replace(word, dict.value[word])

    return line

newTextRDD = textRDD.map(mapWords)\
.saveAsTextFile('./output/text.txt')



sc.stop()