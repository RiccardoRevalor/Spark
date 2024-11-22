#cmd to execute locally: spark-submit --deploy-mode client --master local solution.py <input> <output> <threshold>

import sys
import findspark
findspark.init()
import pyspark
findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('lab06').setMaster('local') 
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

#TASK 1
purchRDD = sc.textFile("data/Purchases.txt").filter(lambda l: l.startswith("2022") or  l.startswith("2023")).cache()
OutputPath1 = "output1"

def UserIDSpending(line):
    #SaleTimestamp,UserID,ItemID,SalePrice
    #return: (UserID, (float) Spending)
    fields = line.split(",")
    return (fields[1], 1)

def calcUsersWithMaxPurchByYear(rdd, year):     
    #calculate max number of purchases in year 2022 for all users
    #select just the lines with the input year
    #calculate max spending for users

    UserIDSpendingRDD = rdd.filter(lambda l: l.startswith(year)).map(UserIDSpending)\
    .reduceByKey(lambda a, b: a+b)

    #(User1, 10)
    #(User2, 20)
    #and so on...

    #return max Spending
    maxSpending =  UserIDSpendingRDD.sortBy(lambda t: t[1], ascending=False).first()[1]
    
    #save the selected users in a file
    return UserIDSpendingRDD.filter(lambda t: t[1] == maxSpending).map(lambda t: t[0])

#for year 2022
RDD2022 = calcUsersWithMaxPurchByYear(purchRDD, "2022")

#for year 2023
RDD2023 = calcUsersWithMaxPurchByYear(purchRDD, "2023")

RDD2022.union(RDD2023).distinct().saveAsTextFile(OutputPath1)



spark.stop()
sc.stop()

    
    