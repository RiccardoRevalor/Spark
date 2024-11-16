import findspark
findspark.init()
import pyspark
findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('ex30').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

#3 inputs
jobContractsPath = "sample_data/JobContracts.txt"
jobOffersPath = "sample_data/JobOffers.txt"
jobPostingsPath = "sample_data/JobPostings.txt"

outputPath1 = "outSpark1/"
outputPath2 = "outSpark2/"

jobPostingsRDD = sc.textFile(jobPostingsPath) #OfferID,JobID,Salary,Status,SS
jobOffersRDD = sc.textFile(jobOffersPath)
jobContractsRDD = sc.textFile(jobContractsPath)


'''
FIRST TASK
Top 3 countries with the highest average salary. For each country, compute the average
salary, considering only accepted job offers. Then, select the top 3 countries with the
highest value of average salary. The first HDFS output file must contain the identifiers of
the selected countries (one country per output line) and their average salary (computed
considering only accepted job offers).
Note: Suppose there is at least an accepted job offer for each country.
'''
def jobIDSalaryOfferID(line):
    '''
    returns (jobID, (salary, offerID))
    Uses file JobOffers.txt
    OfferID,JobID,Salary,Status,SS
    '''
    fields = line.split(",")
    return (fields[1], (float(fields[2]), fields[0]))

#filter the accepted offers and map them to jobIDSalaryOfferID

acceptedRDD = jobOffersRDD\
.filter(lambda jo: jo.split(',')[3] == 'Accepted')\
.map(jobIDSalaryOfferID)


def jobIDCountryTitle(line):
    '''
    returns (jobID, (Country, Title))
    Uses file JobPostings.txt
    JobID,Title,Country
    '''

    fields = line.split(',')
    return (fields[0], (fields[2], fields[1]))

jobIDCountryRDD = jobPostingsRDD.map(jobIDCountryTitle)

'''
JOIN: 
(jobID, (salary, offerID)) JOIN (jobID, (Country, Title)) --> (jobID, ((salary, offerID), (Country, Title))) tupla di tuple
'''

offersWithCountryRDD = acceptedRDD.join(jobIDCountryRDD).cache() #cache it for reusing it in the second task

#Compute average salary for each country in the table
#Map to (Country, (Salary, 1))
CountrySalaryCountRDD = offersWithCountryRDD.map(lambda tupla: (tupla[1][1][0], (tupla[1][0][0], 1)))

#sum the salaries associated with each country
#(Country, (TotalSalary, N))
TotalSalaryCountryRDD = CountrySalaryCountRDD.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) #a, b are the two different entries that get merged


#Compute Avg
#(Country, AvgSalaryPerCountry)
AvgSalaryCountry = TotalSalaryCountryRDD.map(lambda entry: (entry[0], float(entry[1][0]) / float(entry[1][1])))

#TOP-3
top3CountriesPerSalaryList = AvgSalaryCountry.top(3, lambda entry: entry[1])

for c in top3CountriesPerSalaryList: print("Country\n", c)



#Task 2
'''
Select the most popular job title in each country among job postings that resulted in job
contracts (i.e., the job title with the highest number of job contracts in a given country).
Store in the output folder the country names, their most common job title(s), and the
contract count for that job title(s).
Note: For each country, there might be many job titles associated with the highest number
of job contracts for that country. Store all of them in the output folder (one of them per
output line together with country and the contract count).
Note: Suppose there is at least a contract for each country.

'''

def OfferIDCountryTitle(pair):
    #from (jobID, ((salary, offerID), (Country, Title)) ) to (OfferID, (Country, Title))
    return (pair[1][0][1], (pair[1][1][0], pair[1][1][1]))

#map offersWithCountryRDD to OffersCountryTitleRDD 
OffersCountryTitleRDD = offersWithCountryRDD.map(OfferIDCountryTitle).cache()

#Now, take the offers that actually resulted in contracts
def OfferIDNone(line):
    #ContractID,OfferID,ContractDate,ContractType
    #map to (OfferID, None)
    return (line.split(",")[1], None)

actualContractsRDD = jobContractsRDD.map(OfferIDNone)

#join the 2 tables
#(OfferID, ((Country, Title), None))
OffersCountryTitleRDD = OffersCountryTitleRDD.join(actualContractsRDD)

#Map to ((Country, Title), 1) and reduce by counting all the records to count the contracts
CountryTitleNumContractsRDD = OffersCountryTitleRDD.map(lambda tupla: (tupla[1][0], 1))\
.reduceByKey(lambda v1, v2: v1+v2).cache() #at the end I have: ((Title, Country), N)

CountryTitleNumContractsRDD.saveAsTextFile("1.txt")

#now, computer the maximums and th join again to get titles
#since there can be multiple jobs per countru with the maximum number of contracts, it's bettere to compute this nmber first and the list the titles

def CountryNumContracts(line):
    #return (Country, N)
    return (line[0][0], line[1])

CountryContractsMaxRDD = CountryTitleNumContractsRDD.map(CountryNumContracts)\
.reduceByKey(lambda val1, val2: max(val1,val2))


#map (Country, N) to ((Country, N), None)
CountryContractsMaxNoneRDD = CountryContractsMaxRDD.map(lambda tupla: (tupla, None))

def CountryContractsTitle(line):
    #((Country, Title), N) -> ((Country, N), Title)
    return ((line[0][0], line[1]), line[0][1])

#join between CountryContractsTitleRDD and CountryContractsMaxNoneRDD on key = (Country, N)
#to get the titles of the positions with max contracts
#((Country, N), Title)
CountryContractsMaxTitleRDD = CountryTitleNumContractsRDD.map(CountryContractsTitle)\
.join(CountryContractsMaxNoneRDD).map(lambda pair: pair[0][0]+ "," + pair[1][0] + "," + str(pair[0][1]))\
.saveAsTextFile("output_task2")



