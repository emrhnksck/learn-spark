from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('MaxTemperature')
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    tempereture = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32
    return (stationID, entryType, tempereture)

lines = sc.textFile("file:///SparkCourse/data/1800.csv")
parseLines = lines.map(parseLine)
maxTemps = parseLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0],x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = maxTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))