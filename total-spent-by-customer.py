from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("totalSpent")
sc = SparkContext(conf=conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]),float(fields[2]))

input = sc.textFile("file:///D:/SparkCourse/data/customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x,y: x + y)

flipped = totalByCustomer.map(lambda xy: (xy[1], xy[0]))
totalByCustomerSorted = flipped.sortByKey()

results = totalByCustomerSorted.collect()
for result in results:
    print(result)
