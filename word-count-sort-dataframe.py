from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDF = spark.read.text("data/book.txt")

words = inputDF.select(func.explode(func.split(inputDF.value,"\\W+")).alias("word"))

words.filter(words.word != "")

lowerCaseWords = words.select(func.lower(words.word).alias("word"))

wordCounts = lowerCaseWords.groupBy("word").count()

wordCountsSorted = wordCounts.sort("count")

wordCountsSorted.show(wordCountsSorted.count())